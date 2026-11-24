package com.mx.development.jose.said.simulated;

import com.mx.development.jose.said.model.OutboxEvent;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * SIMULATED database.
 *
 * In a real project this would be PostgreSQL (or another database) accessed with JPA/JDBC.
 * Here, every "INSERT / UPDATE" is replaced by a simple log line so the flow is easy to read.
 *
 * The Java collections below only exist so that the demo can check the logic
 * (for example, whether an event ID was already processed). They stand in for tables.
 */
public class FakeDatabase {

    // Stands in for the "outbox" table.
    private final List<OutboxEvent> outboxTable = new ArrayList<>();

    // Stands in for the "processed_events" table.
    // In a real database, the event_id column would be a PRIMARY KEY (unique constraint).
    private final Set<String> processedEventsTable = new HashSet<>();

    // Stands in for the "payments" table.
    private final List<String> paymentsTable = new ArrayList<>();

    // ---------- Transactions (simulated) ----------

    public void beginTransaction() {
        System.out.println("   [DB] BEGIN TRANSACTION");
    }

    public void commit() {
        System.out.println("   [DB] COMMIT");
    }

    public void rollback() {
        System.out.println("   [DB] ROLLBACK");
    }

    // ---------- Outbox table ----------

    public void insertOutboxEvent(OutboxEvent event) {
        // Real SQL: INSERT INTO outbox (event_id, aggregate_id, event_type, payload, status) VALUES (...)
        outboxTable.add(event);
        System.out.println("   [DB] INSERT INTO outbox -> " + event);
    }

    /**
     * Returns events the relay must try to publish, in insertion order.
     *
     * NEW        -> never tried.
     * FAILED     -> a previous attempt failed, so it is retried.
     * PROCESSING -> a previous relay run took it but never finished (it crashed).
     *               Including it here is how a crashed relay's work is picked up again.
     *               (In production this is usually done with a "locked_at" timestamp,
     *               so only rows locked for too long are taken back.)
     */
    public List<OutboxEvent> findPendingOutboxEvents() {
        // Real SQL: SELECT * FROM outbox WHERE status IN ('NEW','FAILED','PROCESSING') ORDER BY id
        List<OutboxEvent> pending = new ArrayList<>();
        for (OutboxEvent e : outboxTable) {
            if (e.status.equals("NEW") || e.status.equals("FAILED") || e.status.equals("PROCESSING")) {
                pending.add(e);
            }
        }
        System.out.println("   [DB] SELECT pending outbox events -> " + pending.size() + " found");
        return pending;
    }

    /** The relay has claimed this event and is about to publish it. */
    public void markOutboxEventAsProcessing(String eventId) {
        // Real SQL: UPDATE outbox SET status = 'PROCESSING' WHERE event_id = ?
        updateStatus(eventId, "PROCESSING");
        System.out.println("   [DB] UPDATE outbox SET status='PROCESSING' WHERE event_id=" + eventId);
    }

    /** Kafka confirmed the publication. The event is done. */
    public void markOutboxEventAsSent(String eventId) {
        // Real SQL: UPDATE outbox SET status = 'SENT', published_at = now() WHERE event_id = ?
        updateStatus(eventId, "SENT");
        System.out.println("   [DB] UPDATE outbox SET status='SENT' WHERE event_id=" + eventId);
    }

    /**
     * Records a failed publish attempt.
     * Increments the attempts counter, stores the error, and sets the status to FAILED.
     * Returns the new number of attempts so the relay can decide whether to give up.
     */
    public int markOutboxEventAsFailed(String eventId, String error) {
        // Real SQL: UPDATE outbox SET status='FAILED', attempts=attempts+1, last_error=? WHERE event_id=?
        for (OutboxEvent e : outboxTable) {
            if (e.eventId.equals(eventId)) {
                e.attempts++;
                e.lastError = error;
                e.status = "FAILED";
                System.out.println("   [DB] UPDATE outbox SET status='FAILED', attempts=" + e.attempts
                        + ", last_error='" + error + "' WHERE event_id=" + eventId);
                return e.attempts;
            }
        }
        return 0;
    }

    /** Too many failed attempts. The event stops being retried and needs manual review. */
    public void markOutboxEventAsDead(String eventId) {
        // Real SQL: UPDATE outbox SET status = 'DEAD' WHERE event_id = ?
        updateStatus(eventId, "DEAD");
        System.out.println("   [DB] UPDATE outbox SET status='DEAD' WHERE event_id=" + eventId
                + " (needs manual review)");
    }

    /** Prints the whole outbox table, one row per line. Useful to see the final state of every event. */
    public void printOutboxTable() {
        // Real SQL: SELECT event_id, aggregate_id, status, attempts, last_error FROM outbox
        System.out.println("   [DB] SELECT * FROM outbox:");
        for (OutboxEvent e : outboxTable) {
            System.out.println("        " + e);
        }
    }

    private void updateStatus(String eventId, String newStatus) {
        for (OutboxEvent e : outboxTable) {
            if (e.eventId.equals(eventId)) {
                e.status = newStatus;
            }
        }
    }

    // ---------- Orders (business table) ----------

    public void insertOrder(String orderId, String customer) {
        // Real SQL: INSERT INTO orders (id, customer) VALUES (...)
        System.out.println("   [DB] INSERT INTO orders -> orderId=" + orderId + ", customer=" + customer);
    }

    // ---------- Processed events (idempotency table) ----------

    /**
     * Tries to record that an event was processed.
     *
     * Returns true  -> the ID was new and has been recorded.
     * Returns false -> the ID already existed (this is a duplicate).
     *
     * In a real database this is an INSERT. If the event_id already exists,
     * the PRIMARY KEY (unique constraint) makes the INSERT fail, and we detect that here.
     */
    public boolean insertProcessedEvent(String eventId) {
        // Real SQL: INSERT INTO processed_events (event_id) VALUES (?)
        // If it fails with a duplicate-key error, the event was already processed.
        boolean isNew = processedEventsTable.add(eventId);
        if (isNew) {
            System.out.println("   [DB] INSERT INTO processed_events -> " + eventId + " (new)");
        } else {
            System.out.println("   [DB] INSERT INTO processed_events -> " + eventId
                    + " FAILED: duplicate key (already processed)");
        }
        return isNew;
    }

    // ---------- Payments (business table on the consumer side) ----------

    public void insertPayment(String orderId) {
        // Real SQL: INSERT INTO payments (order_id, status) VALUES (?, 'PENDING')
        paymentsTable.add(orderId);
        System.out.println("   [DB] INSERT INTO payments -> orderId=" + orderId + ", status=PENDING");
    }

    public int countPayments() {
        return paymentsTable.size();
    }
}
