package edu.eci.arsw.kafka.dto;

import java.time.Instant;

public class NotificationSentEvent {

    private String notificationId;
    private String orderId;
    private String customerId;
    private String message;
    private String status;
    private Instant occurredAt;

    public NotificationSentEvent() {
    }

    public NotificationSentEvent(String notificationId, String orderId, String customerId, String message, String status, Instant occurredAt) {
        this.notificationId = notificationId;
        this.orderId = orderId;
        this.customerId = customerId;
        this.message = message;
        this.status = status;
        this.occurredAt = occurredAt;
    }

    public String getNotificationId() {
        return notificationId;
    }

    public void setNotificationId(String notificationId) {
        this.notificationId = notificationId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(Instant occurredAt) {
        this.occurredAt = occurredAt;
    }
}
