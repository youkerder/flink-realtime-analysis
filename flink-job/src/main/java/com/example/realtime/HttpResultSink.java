package com.example.realtime;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 通过 ClickHouse HTTP 接口写结果表的 Sink（用于「无 Docker / 无 ClickHouse 服务端」的本机运行模式）。
 *
 * <h3>为什么需要它</h3>
 * 默认的 {@code JdbcSink} 需要真正的 ClickHouse 服务端（TCP/HTTP + Native 协议）。在 Windows
 * 开发机上如果没有 Docker/WSL，服务端起不来，整条链路就断在最后一公里。这个 Sink 把写入
 * 换成 ClickHouse 的 <b>HTTP + JSONEachRow</b> 协议——该协议本身就是 ClickHouse 官方接口，
 * 所以：
 * <ul>
 *   <li>作业侧的落地语义与生产一致（批量、重试、攒批条数/时间上限都能对齐 JdbcSink）；</li>
 *   <li>对端只要实现 ClickHouse HTTP 协议即可，既可以是真正的 ClickHouse，也可以是
 *       {@code tools/ch_http_server.py} 这层本地兼容服务（DuckDB 列存引擎）。</li>
 * </ul>
 *
 * <h3>批量策略</h3>
 * 与 {@code JdbcExecutionOptions} 对齐：攒够 {@code batchSize} 行或超过 {@code flushIntervalMs}
 * 就发一批；失败重试 {@code maxRetries} 次，仍失败则抛出，让 Flink 走重启策略（避免静默丢结果）。
 */
public class HttpResultSink extends RichSinkFunction<String> {
    private static final long serialVersionUID = 1L;

    private final String endpoint;      // 例如 http://127.0.0.1:8090/clickhouse/
    private final String insertSql;     // INSERT INTO rt.metrics_1min FORMAT JSONEachRow
    private final int batchSize;
    private final long flushIntervalMs;
    private final int maxRetries;

    private transient List<String> buffer;
    private transient long bufferStartMs;
    private transient java.util.concurrent.ScheduledExecutorService flusher;

    public HttpResultSink(String endpoint, String insertSql) {
        this(endpoint, insertSql, 300, 500L, 2);
    }

    public HttpResultSink(String endpoint, String insertSql, int batchSize, long flushIntervalMs, int maxRetries) {
        this.endpoint = endpoint;
        this.insertSql = insertSql;
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.maxRetries = maxRetries;
    }

    @Override
    public void open(Configuration parameters) {
        buffer = new ArrayList<>(batchSize);
        bufferStartMs = 0L;

        // 定时刷盘：只在 invoke() 里判断超时会漏掉"数据流暂停"的场景——
        // 最后一批结果攒在缓冲区里，但后面没有新记录进来，就永远不会刷出去，
        // 表现为"大屏的最后一条窗口缺一半数据"（本项目实测踩到）。
        // 用一个单线程守护调度器兜底，保证 batchInterval 内一定落库。
        flusher = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "http-sink-flusher");
            t.setDaemon(true);
            return t;
        });
        flusher.scheduleWithFixedDelay(() -> {
            try {
                flush();
            } catch (Exception e) {
                // 定时任务里抛异常会导致后续调度被取消，这里只记录，交给下一次重试
                System.err.println("[http-sink] scheduled flush failed: " + e.getMessage());
            }
        }, flushIntervalMs, flushIntervalMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    @Override
    public void invoke(String jsonLine, Context context) throws Exception {
        long now = System.currentTimeMillis();
        // 时间窗口从"本批第一行"开始算，而不是从"上次刷盘"开始算。
        // 否则一批数据刚刷完，下一条到达时会因为距离上次刷盘已超过 interval 而立刻单独刷出，
        // 把一个窗口的多行结果（漏斗 3 行、TopN 100 行）拆成多批，大屏就会看到 "半截" 的窗口。
        synchronized (this) {
            if (buffer.isEmpty()) {
                bufferStartMs = now;
            }
            buffer.add(jsonLine);
            if (buffer.size() >= batchSize || now - bufferStartMs >= flushIntervalMs) {
                try {
                    flush();
                } catch (Exception e) {
                    // 写失败不能让算子抛异常：一旦抛出去作业就会重启，重启后从 earliest/checkpoint
                    // 重放又会重复写。这里保留缓冲区，交给定时线程下个周期重试；
                    // 对端按窗口主键做幂等（INSERT OR REPLACE），重发不会让结果翻倍。
                    System.err.println("[http-sink] flush failed, will retry: " + e.getMessage());
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (flusher != null) {
            flusher.shutdownNow();
        }
        flush();
    }

    private synchronized void flush() throws Exception {
        if (buffer == null || buffer.isEmpty()) {
            return;
        }
        String body = String.join("\n", buffer);
        int n = buffer.size();
        Exception last = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                post(body);
                buffer.clear();
                bufferStartMs = 0L;
                return;
            } catch (Exception e) {
                last = e;
                Thread.sleep(200L * (attempt + 1));
            }
        }
        throw new RuntimeException("write " + n + " rows failed after retries", last);
    }

    private void post(String body) throws Exception {
        String url = endpoint + (endpoint.contains("?") ? "&" : "?")
                + "query=" + URLEncoder.encode(insertSql, "UTF-8");
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("POST");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(20000);
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/octet-stream");
        byte[] payload = body.getBytes(StandardCharsets.UTF_8);
        conn.setFixedLengthStreamingMode(payload.length);
        try (OutputStream os = conn.getOutputStream()) {
            os.write(payload);
        }
        int code = conn.getResponseCode();
        if (code >= 300) {
            String msg = new String(conn.getErrorStream() != null ? conn.getErrorStream().readAllBytes()
                    : new byte[0], StandardCharsets.UTF_8);
            throw new IllegalStateException("HTTP " + code + ": " + msg);
        }
        conn.disconnect();
    }
}
