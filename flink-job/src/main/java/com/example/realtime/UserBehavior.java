package com.example.realtime;

/** 用户行为事件（对应 Kafka 消息体），POJO 序列化要求：公共无参构造 + getter/setter。 */
public class UserBehavior {
    private long userId;
    private long itemId;
    private long categoryId;
    private String behavior;   // pv / cart / fav / buy
    private long timestamp;    // 事件时间（秒）

    public UserBehavior() {
    }

    public UserBehavior(long userId, long itemId, long categoryId, String behavior, long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public long getUserId() { return userId; }
    public void setUserId(long userId) { this.userId = userId; }

    public long getItemId() { return itemId; }
    public void setItemId(long itemId) { this.itemId = itemId; }

    public long getCategoryId() { return categoryId; }
    public void setCategoryId(long categoryId) { this.categoryId = categoryId; }

    public String getBehavior() { return behavior; }
    public void setBehavior(String behavior) { this.behavior = behavior; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}
