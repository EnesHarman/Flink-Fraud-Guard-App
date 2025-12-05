package com.fraudguard.model;

public class Transaction {
    private String transactionId;
    private String userId;
    private double amount;
    private long timestamp; // Event Time
    private double lat;
    private double lon;

    // Default constructor is crucial for Flink POJO serialization
    public Transaction() {}

    public Transaction(String transactionId, String userId, double amount, long timestamp, double lat, double lon) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.amount = amount;
        this.timestamp = timestamp;
        this.lat = lat;
        this.lon = lon;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "userId='" + userId + '\'' +
                ", amount=" + amount +
                ", timestamp=" + timestamp +
                '}';
    }
}
