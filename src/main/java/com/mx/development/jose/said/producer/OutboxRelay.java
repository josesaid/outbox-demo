package com.mx.development.jose.said.producer;

import com.mx.development.jose.said.model.OutboxEvent;
import com.mx.development.jose.said.simulated.FakeDatabase;
import com.mx.development.jose.said.simulated.FakeKafka;

import java.util.List;

/**
 * PRODUCER SIDE - step 2: the RELAY reads the outbox and publishes to Kafka.
 *
 * In a real project this is usually a scheduled job (@Scheduled), or a CDC tool such as Debezium.
 * It runs separately from the business transaction.
 *
 * Life cycle of one event handled by this class:
 *
 *   NEW ──► PROCESSING ──► SENT                      (happy path)
 *                 │
 *                 └──► FAILED ──► PROCESSING ──► ...  (Kafka error: retry on next run)
 *                            │
 *                            └──► DEAD                 (attempts reached MAX_ATTEMPTS)
 *
 * Important: the relay may publish an event and then crash BEFORE marking it as SENT.
 * In that case the event stays in PROCESSING and is published again on the next run.
 * That is "at-least-once delivery", and it is why the consumer must be idempotent.
 */
public class OutboxRelay {

    // After this many failed attempts the event is marked DEAD and no longer retried.
    // Real systems usually also wait between attempts (exponential backoff); see next_retry_at in the README.
    public static final int MAX_ATTEMPTS = 3;

    private final FakeDatabase db;
    private final FakeKafka kafka;

    // Demo switch: when true, the relay "crashes" after publishing and before marking as SENT.
    private boolean simulateCrashAfterPublish = false;

    public OutboxRelay(FakeDatabase db, FakeKafka kafka) {
        this.db = db;
        this.kafka = kafka;
    }

    public void setSimulateCrashAfterPublish(boolean value) {
        this.simulateCrashAfterPublish = value;
    }

    /** Publishes all pending outbox events to Kafka, in order. */
    public void publishPendingEvents() {
        System.out.println("\n[RELAY] running...");

        List<OutboxEvent> pending = db.findPendingOutboxEvents();

        if (pending.isEmpty()) {
            System.out.println("[RELAY] nothing to publish");
            return;
        }

        for (OutboxEvent event : pending) {
            // 1) Claim the event so other relay instances do not publish it at the same time.
            db.markOutboxEventAsProcessing(event.eventId);

            // 2) Try to send to Kafka. The aggregateId is the KEY, so events of the same order stay in order.
            try {
                kafka.send(event.aggregateId, event.eventId, event.payload);
            } catch (RuntimeException ex) {
                // Kafka failed: record the attempt, then decide between retry and giving up.
                int attempts = db.markOutboxEventAsFailed(event.eventId, ex.getMessage());
                if (attempts >= MAX_ATTEMPTS) {
                    System.out.println("   [RELAY] attempts=" + attempts + " reached MAX_ATTEMPTS="
                            + MAX_ATTEMPTS + " -> giving up on eventId=" + event.eventId);
                    db.markOutboxEventAsDead(event.eventId);
                } else {
                    System.out.println("   [RELAY] will retry eventId=" + event.eventId
                            + " on the next run (attempt " + attempts + " of " + MAX_ATTEMPTS + ")");
                }
                // Move on to the next event; this one is handled.
                continue;
            }

            // Demo only: simulate a crash right after publishing.
            // The event stays in PROCESSING, so the next run publishes it again.
            if (simulateCrashAfterPublish) {
                System.out.println("   [RELAY] !!! simulated CRASH after publish, before marking SENT");
                throw new RuntimeException("Simulated relay crash");
            }

            // 3) Kafka accepted the message. Mark it as SENT so it is not published again.
            db.markOutboxEventAsSent(event.eventId);
        }
    }
}
