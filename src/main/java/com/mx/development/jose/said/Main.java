package com.mx.development.jose.said;

import com.mx.development.jose.said.consumer.PaymentConsumer;
import com.mx.development.jose.said.producer.OrderService;
import com.mx.development.jose.said.producer.OutboxRelay;
import com.mx.development.jose.said.simulated.FakeDatabase;
import com.mx.development.jose.said.simulated.FakeKafka;

/**
 * Demo entry point. Run it and read the log from top to bottom.
 *
 * Scenarios:
 *  A) Normal flow: create an order, the relay publishes it, the consumer processes it once.
 *  B) Relay crash: the relay publishes but crashes before marking SENT.
 *     The event is published again, and the consumer SKIPS the duplicate.
 *  C) Consumer crash: the consumer commits the DB but crashes before the offset commit.
 *     Kafka redelivers the message, and the consumer SKIPS it because it was already processed.
 *  D) Temporary broker failure: Kafka fails twice, the event goes FAILED -> FAILED -> SENT.
 *  E) Permanent broker failure: Kafka keeps failing, the event goes FAILED -> FAILED -> DEAD.
 */
public class Main {

    public static void main(String[] args) {
        FakeDatabase db = new FakeDatabase();
        FakeKafka kafka = new FakeKafka();

        OrderService orderService = new OrderService(db);
        OutboxRelay relay = new OutboxRelay(db, kafka);
        PaymentConsumer consumer = new PaymentConsumer(db, kafka);

        System.out.println("========== SCENARIO A: normal flow ==========");
        orderService.createOrder("Alice");   // producer writes order + event in one transaction
        relay.publishPendingEvents();        // relay sends the event to Kafka (NEW -> PROCESSING -> SENT)
        consumer.handle(0);                  // consumer processes offset 0

        System.out.println("\n========== SCENARIO B: relay crashes after publish ==========");
        orderService.createOrder("Bob");
        relay.setSimulateCrashAfterPublish(true);
        try {
            relay.publishPendingEvents();    // publishes, then crashes; the event stays PROCESSING
        } catch (RuntimeException ex) {
            System.out.println("[MAIN] relay failed: " + ex.getMessage());
        }
        relay.setSimulateCrashAfterPublish(false);
        relay.publishPendingEvents();        // next run picks up the PROCESSING event and publishes it again
        consumer.handle(1);                  // processes Bob's order
        consumer.handle(2);                  // duplicate of Bob's event -> SKIPPED

        System.out.println("\n========== SCENARIO C: consumer crashes before offset commit ==========");
        orderService.createOrder("Carol");
        relay.publishPendingEvents();
        consumer.setSimulateCrashBeforeOffsetCommit(true);
        try {
            consumer.handle(3);              // processed in DB, then crashes before offset commit
        } catch (RuntimeException ex) {
            System.out.println("[MAIN] consumer failed: " + ex.getMessage());
        }
        consumer.setSimulateCrashBeforeOffsetCommit(false);
        System.out.println("[MAIN] Kafka redelivers offset 3 after restart...");
        consumer.handle(3);                  // redelivery -> SKIPPED, no second payment

        System.out.println("\n========== SCENARIO D: temporary broker failure (2 failures, then OK) ==========");
        orderService.createOrder("Dave");
        kafka.setFailuresToSimulate(2);      // the next two sends will fail
        relay.publishPendingEvents();        // attempt 1 fails -> FAILED, attempts=1
        relay.publishPendingEvents();        // attempt 2 fails -> FAILED, attempts=2
        relay.publishPendingEvents();        // attempt 3 succeeds -> SENT
        consumer.handle(4);                  // processes Dave's order

        System.out.println("\n========== SCENARIO E: permanent broker failure (gives up after 3 attempts) ==========");
        orderService.createOrder("Eve");
        kafka.setFailuresToSimulate(100);    // Kafka keeps failing
        relay.publishPendingEvents();        // attempt 1 -> FAILED
        relay.publishPendingEvents();        // attempt 2 -> FAILED
        relay.publishPendingEvents();        // attempt 3 -> DEAD (MAX_ATTEMPTS reached)
        relay.publishPendingEvents();        // nothing left to publish: DEAD events are not retried
        kafka.setFailuresToSimulate(0);      // the broker is back, but Eve's event is already DEAD

        System.out.println("\n========== OUTBOX FINAL STATE ==========");
        db.printOutboxTable();

        System.out.println("\n========== RESULT ==========");
        System.out.println("Payments created: " + db.countPayments()
                + " (expected 4: Alice, Bob, Carol, Dave; Eve never reached Kafka)");
    }
}
