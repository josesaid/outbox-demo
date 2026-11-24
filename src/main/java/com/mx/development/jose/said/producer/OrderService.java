package com.mx.development.jose.said.producer;

import com.mx.development.jose.said.model.OutboxEvent;
import com.mx.development.jose.said.simulated.FakeDatabase;

import java.util.UUID;

/**
 * PRODUCER SIDE - step 1: save the business change AND the event together.
 *
 * This is the core of the TRANSACTIONAL OUTBOX pattern:
 *  - We do NOT send to Kafka here.
 *  - We insert the order and the event in the SAME database transaction.
 *  - If anything fails, both inserts are rolled back, so we never have an order without an event
 *    (or an event without an order).
 */
public class OrderService {

    private final FakeDatabase db;

    public OrderService(FakeDatabase db) {
        this.db = db;
    }

    public String createOrder(String customer) {
        // A business ID for the order. It is also the aggregateId of the event.
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8);

        // A unique ID for THIS event. The consumer will use it to detect duplicates.
        String eventId = "EVT-" + UUID.randomUUID();

        System.out.println("\n[PRODUCER] createOrder for customer=" + customer + " orderId=" + orderId);

        // ---- Everything between begin and commit is ONE database transaction ----
        db.beginTransaction();
        try {
            // 1) The business change: save the order.
            db.insertOrder(orderId, customer);

            // 2) The event: save it in the outbox table in the SAME transaction.
            //    Nothing is sent to Kafka yet. The OutboxRelay will do that later.
            OutboxEvent event = new OutboxEvent(eventId, orderId, "OrderCreated",
                    "{orderId=" + orderId + ", customer=" + customer + "}");
            db.insertOutboxEvent(event);

            db.commit();
        } catch (RuntimeException ex) {
            // If anything fails, the order AND the event are both rolled back.
            db.rollback();
            throw ex;
        }

        return orderId;
    }
}
