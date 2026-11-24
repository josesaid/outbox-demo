package com.mx.development.jose.said.consumer;

import com.mx.development.jose.said.simulated.FakeDatabase;
import com.mx.development.jose.said.simulated.FakeKafka;

/**
 * CONSUMER SIDE: idempotent processing of "OrderCreated" messages.
 *
 * Idempotent means: processing the same message twice has the same effect as processing it once.
 *
 * The pattern used here:
 *  1. Start a database transaction.
 *  2. Try to INSERT the eventId into "processed_events".
 *     - If the INSERT fails (duplicate key), the event was already processed -> skip it.
 *  3. If the INSERT works, do the business work (insert a payment) in the SAME transaction.
 *  4. Commit the transaction. Only then commit the Kafka offset.
 *
 * Because steps 2 and 3 are in one transaction, a crash cannot leave the database half-updated.
 */
public class PaymentConsumer {

    private final FakeDatabase db;
    private final FakeKafka kafka;

    // Demo switch: when true, the consumer "crashes" after the DB commit and before the offset commit.
    private boolean simulateCrashBeforeOffsetCommit = false;

    public PaymentConsumer(FakeDatabase db, FakeKafka kafka) {
        this.db = db;
        this.kafka = kafka;
    }

    public void setSimulateCrashBeforeOffsetCommit(boolean value) {
        this.simulateCrashBeforeOffsetCommit = value;
    }

    /**
     * Processes ONE message at the given offset.
     * In real Spring Kafka, this method would be annotated with @KafkaListener.
     */
    public void handle(int offset) {
        FakeKafka.Record record = kafka.read(offset);
        if (record == null) {
            System.out.println("[CONSUMER] no message at offset " + offset);
            return;
        }

        System.out.println("\n[CONSUMER] received offset=" + offset
                + " key=" + record.key + " eventId=" + record.eventId);

        db.beginTransaction();
        try {
            // STEP 2 - Idempotency check. This is the "if" you described, but done as an INSERT.
            // Because processed_events.event_id is UNIQUE, two consumers can not both insert it.
            boolean isNew = db.insertProcessedEvent(record.eventId);

            if (!isNew) {
                // Duplicate: someone already processed this event. Do nothing.
                System.out.println("[CONSUMER] duplicate detected -> SKIP eventId=" + record.eventId);
                db.rollback();
                kafka.commitOffset(offset); // The message is done, so we move on.
                return;
            }

            // STEP 3 - The real business work, in the SAME transaction as the processed-events insert.
            db.insertPayment(record.key);

            db.commit();
            System.out.println("[CONSUMER] processed eventId=" + record.eventId);

        } catch (RuntimeException ex) {
            // Any error rolls back BOTH the processed-event row and the payment.
            db.rollback();
            throw ex;
        }

        // Demo only: crash after the database commit, before the Kafka offset commit.
        if (simulateCrashBeforeOffsetCommit) {
            System.out.println("[CONSUMER] !!! simulated CRASH after DB commit, before offset commit");
            throw new RuntimeException("Simulated consumer crash");
        }

        // STEP 4 - Only now we tell Kafka that this message is done.
        kafka.commitOffset(offset);
    }
}
