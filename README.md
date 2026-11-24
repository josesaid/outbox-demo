# Transactional Outbox + Idempotent Consumer (Java demo)

![Java](https://img.shields.io/badge/Java-17%2B-ED8B00?logo=openjdk&logoColor=white) ![Kafka](https://img.shields.io/badge/Kafka-Simulated-231F20?logo=apachekafka&logoColor=white) ![Database](https://img.shields.io/badge/Database-Simulated-4169E1?logo=postgresql&logoColor=white) ![Dependencies](https://img.shields.io/badge/Dependencies-None-success)

**Project page:** https://josesaid.github.io/outbox-demo/

This demo shows two patterns that work together in event-driven systems:

1. **Transactional outbox (producer side):** the business change and the event are saved in the same database transaction. A relay publishes pending events to Kafka afterwards.
2. **Idempotent consumer (consumer side):** each event ID is recorded in a `processed_events` table inside the same transaction as the business work. Duplicates are detected and skipped.

Kafka and the database are **simulated** with plain Java classes that print log lines. No external libraries are needed.

## Project structure

```
src/main/java/com/mx/development/jose/said/
├── Main.java                       # Entry point with five scenarios (A to E)
├── model/OutboxEvent.java          # One row of the outbox table
├── simulated/FakeDatabase.java     # Simulated DB (orders, outbox, processed_events, payments)
├── simulated/FakeKafka.java        # Simulated topic (a list, with offsets)
├── producer/OrderService.java      # Step 1: order + event in one transaction
├── producer/OutboxRelay.java       # Step 2: reads the outbox and publishes to Kafka
└── consumer/PaymentConsumer.java   # Idempotent processing of OrderCreated events
```

## Diagrams

Each language has an overview page (the full path of one event, the outbox states, and the five scenarios) and one sequence diagram per scenario in `Main`.

| Page | English | Español |
|---|---|---|
| Overview | [workflow-english](https://josesaid.github.io/outbox-demo/diagrams/english/workflow-english.html) | [flujo-español](https://josesaid.github.io/outbox-demo/diagrams/spanish/flujo-espa%C3%B1ol.html) |
| A · Normal flow | [scenario-a-normal-flow](https://josesaid.github.io/outbox-demo/diagrams/english/scenario-a-normal-flow.html) | [escenario-a-flujo-normal](https://josesaid.github.io/outbox-demo/diagrams/spanish/escenario-a-flujo-normal.html) |
| B · Relay crash | [scenario-b-relay-crash](https://josesaid.github.io/outbox-demo/diagrams/english/scenario-b-relay-crash.html) | [escenario-b-caida-del-relay](https://josesaid.github.io/outbox-demo/diagrams/spanish/escenario-b-caida-del-relay.html) |
| C · Consumer crash | [scenario-c-consumer-crash](https://josesaid.github.io/outbox-demo/diagrams/english/scenario-c-consumer-crash.html) | [escenario-c-caida-del-consumidor](https://josesaid.github.io/outbox-demo/diagrams/spanish/escenario-c-caida-del-consumidor.html) |
| D · Temporary broker failure | [scenario-d-temporary-broker-failure](https://josesaid.github.io/outbox-demo/diagrams/english/scenario-d-temporary-broker-failure.html) | [escenario-d-falla-temporal-de-kafka](https://josesaid.github.io/outbox-demo/diagrams/spanish/escenario-d-falla-temporal-de-kafka.html) |
| E · Permanent broker failure | [scenario-e-permanent-broker-failure](https://josesaid.github.io/outbox-demo/diagrams/english/scenario-e-permanent-broker-failure.html) | [escenario-e-falla-permanente-de-kafka](https://josesaid.github.io/outbox-demo/diagrams/spanish/escenario-e-falla-permanente-de-kafka.html) |

The source files live in [`docs/diagrams/english/`](docs/diagrams/english/) and [`docs/diagrams/spanish/`](docs/diagrams/spanish/), which GitHub Pages publishes.

## How to run

Requires Java 17 or newer.

```bash
git clone https://github.com/josesaid/outbox-demo.git
cd outbox-demo
javac -d out $(find src -name "*.java")
java -cp out com.mx.development.jose.said.Main
```

## What each scenario shows

| Scenario | What happens | Expected result |
|---|---|---|
| **A. Normal flow** | Order created, relay publishes, consumer processes offset 0. | 1 payment. |
| **B. Relay crash** | The relay publishes Bob's event, then crashes before marking it `SENT`. The next run publishes the same event again. The consumer processes the first copy and **skips the duplicate**. | 1 payment for Bob, the duplicate is skipped. |
| **C. Consumer crash** | The consumer commits the payment and the processed-event row, then crashes before committing the Kafka offset. Kafka redelivers the message, and the consumer **skips it** because the event ID is already recorded. | 1 payment for Carol, no second payment. |
| **D. Temporary broker failure** | Kafka fails twice. The event goes `FAILED` (attempts 1), then `FAILED` (attempts 2), then `SENT` on the third run. | 1 payment for Dave. |
| **E. Permanent broker failure** | Kafka keeps failing. The event goes `FAILED` (attempts 1), `FAILED` (attempts 2), then `DEAD` (attempts 3 = `MAX_ATTEMPTS`). A `DEAD` event is never retried. | No payment for Eve. |

The final line should read `Payments created: 4`. The last section of the log prints the whole outbox table, so you can see each event's final status, attempts and last error.

## Outbox states

```
NEW ──► PROCESSING ──► SENT                       (happy path)
              │
              └──► FAILED ──► PROCESSING ──► ...   (Kafka error, retried on next run)
                         │
                         └──► DEAD                  (attempts reached MAX_ATTEMPTS = 3)
```

| Status | Meaning |
|---|---|
| `NEW` | Written with the order, never tried. |
| `PROCESSING` | Claimed by the relay. If it stays here, the relay crashed mid-publish and the event is picked up again. |
| `SENT` | Kafka accepted the message. Final state. |
| `FAILED` | The last attempt failed. `attempts` and `lastError` are updated. Retried on the next run. |
| `DEAD` | Too many failed attempts. Final state; needs manual review or a reprocess job. |

In a real database, `PROCESSING` usually comes with a `locked_at` timestamp, and `FAILED` with a `next_retry_at` for exponential backoff. The demo skips both to keep the flow easy to follow.

## Key ideas to look for in the code

- **Same transaction on the producer side:** `OrderService.createOrder` writes the order and the outbox row in one transaction. Nothing is sent to Kafka inside it.
- **At-least-once delivery:** `OutboxRelay` can publish an event more than once. That is why the consumer must be idempotent.
- **Idempotency check inside the transaction:** `PaymentConsumer.handle` inserts into `processed_events` first. A unique key makes the second insert fail, and the business write is rolled back with it.
- **Message key = aggregate ID:** Kafka keeps order only within a partition, so all events of the same order use the same key.
- **Retries with a limit:** `OutboxRelay` counts failed attempts and gives up at `MAX_ATTEMPTS`, moving the event to `DEAD` instead of retrying forever.
- **Offset commit last:** the offset is committed only after the database commit. If the consumer crashes in between, the message is redelivered and handled safely.

## Mapping to real technologies

| Simulated here | Real project |
|---|---|
| `FakeDatabase` | PostgreSQL with JPA/JDBC (`@Transactional`) |
| `processed_events` set | Table with `event_id` as `PRIMARY KEY` |
| `FakeKafka.send` | `KafkaTemplate.send(topic, key, value)` |
| `OutboxRelay.publishPendingEvents` | `@Scheduled` job, or CDC with Debezium |
| `PaymentConsumer.handle` | `@KafkaListener` with manual acknowledgment |

## Stack

| Item | Value |
|---|---|
| Language | Java 17+ (plain JDK, no build tool) |
| Framework | Not used (no Spring) |
| Database | Simulated in memory (`FakeDatabase`) |
| Messaging | Simulated Kafka topic (`FakeKafka`) |
| Docker / Cloud | Not used |
| Configuration | No environment variables needed |

## Tests and limitations

- There are no unit tests; `Main` runs five scenarios and prints `Payments created: 4` when everything works.
- Kafka and the database are in-memory simulations, so transactions, locks and partitions are modeled, not real.
- More projects: https://josesaid.github.io/
