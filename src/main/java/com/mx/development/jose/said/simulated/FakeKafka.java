package com.mx.development.jose.said.simulated;

import java.util.ArrayList;
import java.util.List;

/**
 * SIMULATED Kafka topic.
 *
 * In a real project this would be a KafkaTemplate (producer) and a @KafkaListener (consumer).
 * Here, the topic is a simple list. Each record is added at the end, which mirrors
 * how a Kafka partition keeps messages in order.
 */
public class FakeKafka {

    /** One message stored in the simulated topic. */
    public static class Record {
        public final String key;      // Kafka message key, here the orderId
        public final String eventId;  // Unique event ID, sent as a header in real Kafka
        public final String payload;

        public Record(String key, String eventId, String payload) {
            this.key = key;
            this.eventId = eventId;
            this.payload = payload;
        }
    }

    private final List<Record> topic = new ArrayList<>();

    // Demo switch: the next N calls to send() fail, as if the broker were unavailable.
    private int failuresToSimulate = 0;

    /** Demo only: makes the next {@code count} sends fail. */
    public void setFailuresToSimulate(int count) {
        this.failuresToSimulate = count;
    }

    /** Producer side: sends a message to the topic. Throws if the broker is "down". */
    public void send(String key, String eventId, String payload) {
        if (failuresToSimulate > 0) {
            failuresToSimulate--;
            System.out.println("   [KAFKA] !!! send FAILED for eventId=" + eventId + " (broker unavailable)");
            // Real code: a TimeoutException or NotLeaderForPartitionException from the producer.
            throw new RuntimeException("Broker unavailable");
        }
        // Real code: kafkaTemplate.send("orders.events", key, payload) with the eventId as a header
        topic.add(new Record(key, eventId, payload));
        System.out.println("   [KAFKA] send to topic 'orders.events' key=" + key + " eventId=" + eventId);
    }

    /** Consumer side: returns the message stored at this offset, or null if there is none. */
    public Record read(int offset) {
        if (offset < topic.size()) {
            return topic.get(offset);
        }
        return null;
    }

    /** Consumer side: logs that the offset was committed. In real Kafka this tells the broker "I am done". */
    public void commitOffset(int offset) {
        // Real code: the consumer commits the offset (automatically or manually with ack.acknowledge())
        System.out.println("   [KAFKA] commit offset " + offset);
    }
}
