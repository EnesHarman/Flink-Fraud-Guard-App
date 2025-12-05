package com.fraudguard.model;

public class Alert {
    public String alertId;
    public String transactionId;
    public String userId;
    public String alertType;
    public String message;

    public Alert() {}

    public Alert(String transactionId, String userId, String alertType, String message) {
        this.alertId = java.util.UUID.randomUUID().toString();
        this.transactionId = transactionId;
        this.userId = userId;
        this.alertType = alertType;
        this.message = message;
    }

    @Override
    public String toString() {
        return "!!! ALERT [" + alertType + "] User: " + userId + " -> " + message;
    }
}
