package com.mx.development.jose.said.model;

/**
 * One row of the "outbox" table.
 *
 * The outbox table stores events that must be published to Kafka.
 * The event is written in the SAME database transaction as the business change
 * (for example, creating an order). This way, both are saved together or not at all.
 */
public class OutboxEvent {

    // Unique ID of the event. The consumer uses it to detect duplicates.
    public final String eventId;

    // Business key of the entity. We use it as the Kafka message key,
    // so all events for the same order land in the same partition and keep their order.
    public final String aggregateId;

    public final String eventType;

    // Simple text payload for the demo. In a real project this would be JSON.
    public final String payload;

    // Lifecycle of the row. See the state diagram in the README:
    //   NEW -> PROCESSING -> SENT
    //   PROCESSING -> FAILED -> PROCESSING ... -> DEAD (after MAX_ATTEMPTS)
    public String status;

    // Number of failed publish attempts. The relay stops retrying when it reaches the maximum.
    public int attempts;

    // Text of the last error returned by Kafka, kept for debugging.
    public String lastError;

    public OutboxEvent(String eventId, String aggregateId, String eventType, String payload) {
        this.eventId = eventId;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.status = "NEW";
        this.attempts = 0;
        this.lastError = null;
    }

    @Override
    public String toString() {
        return "OutboxEvent{eventId=" + eventId + ", aggregateId=" + aggregateId
                + ", type=" + eventType + ", status=" + status
                + ", attempts=" + attempts + ", lastError=" + lastError + "}";
    }
}
