# WIL (What I Learned) — PR #2

Repository: GURUVENKATESH03/kafka-learnings
PR: #2 — ref - Changed the command line runner to a Kafka Listener and Kafka Controller

## In a nutshell
- Converted the app from producing a single message via CommandLineRunner to a proper HTTP-triggered producer (KafkaController) and a dedicated Kafka consumer component (kafka.demo.listener.KafkaListener).
- Added a lightweight DTO (ResponseCreater<T>) to carry data and a single REST endpoint that fetches external data and publishes it to Kafka.
- Removed the inline producer and in-class @KafkaListener from DemoApplication and moved responsibilities into separate components.

## PR key facts
- Author: GURUVENKATESH03
- State: merged (merged_at: 2026-09-12T13:33:30Z)
- Commits: 1
- Files changed: 4
- Additions: 86, Deletions: 14
- Changed files:
  - src/main/java/kafka/demo/DemoApplication.java — removed CommandLineRunner producer and inline KafkaListener
  - src/main/java/kafka/demo/controller/KafkaController.java — NEW: Rest controller that fetches data from a target URL and publishes to topic `ApiLogs` using KafkaTemplate and ProducerRecord
  - src/main/java/kafka/demo/listener/KafkaListener.java — NEW: Kafka consumer component that listens on topic `ApiLogs` (via ResponseCreater.KAFKA_TOPIC) and logs messages
  - src/main/java/kafka/demo/utils/ResponseCreater.java — NEW: Lombok-based DTO/builder and KAFKA_TOPIC constant

## What I learned (from making and reviewing these changes)
- How to move from a one-off CommandLineRunner producer to a reusable HTTP endpoint that produces Kafka messages.
- Using Spring's KafkaTemplate to send ProducerRecord objects instead of only simple key/value send(...) overloads.
- How to set HTTP request headers with RestTemplate.exchange to avoid 403s when pulling remote content.
- Creating a small DTO (ResponseCreater) with Lombok (@Data, @Builder) to wrap payloads and a shared topic constant.
- Implementing a Spring component consumer with @KafkaListener and logging incoming messages with SLF4J.
- Keeping application responsibilities separated: controller (produce), listener (consume), application class (bootstrap).
- Basic use of ProducerRecord, RestTemplate, and KafkaTemplate together in a Spring Boot app.

## Code/design notes & small observations
- The controller uses RestTemplate and sets a User-Agent header to avoid being blocked by some servers; good pragmatic step but consider timeouts and error handling.
- ResponseCreater is generic and simple; it currently stores the entire external response as `data` — consider serialization format and message size limitations.
- KafkaListener currently logs the ResponseCreater object directly; for production you'd deserialize/validate and handle business logic.
- DemoApplication now only boots the app — that's a clearer separation of concerns.

## What needs to be learned next (recommended follow-ups)
Priority order with why and short tasks:

1) Message serialization and schema management (JSON vs Avro/Protobuf)
   - Why: Response body is being forwarded as-is; using schemas reduces versioning and parsing issues.
   - Tasks: learn about Spring Kafka serializers/deserializers, set up JSON serde, or integrate Avro + Schema Registry.

2) Error handling, retries, and timeouts for REST fetch and Kafka sends
   - Why: network calls and publish may fail; the controller currently has no retry/backoff or timeout configuration.
   - Tasks: configure RestTemplate timeouts, use Spring Retry for fetch/publish, check KafkaTemplate callbacks for send failures.

3) Integration testing with Embedded Kafka
   - Why: validate end-to-end behaviour (controller -> Kafka -> listener) in CI without external Kafka infra.
   - Tasks: add spring-kafka-test dependency and write an integration test that publishes and asserts consumption.

4) Consumer configuration: groups, offsets, concurrency, and idempotence
   - Why: ensure consumers scale and are resilient to reprocessing.
   - Tasks: learn groupId handling, concurrency config on @KafkaListener, manual vs auto-commit, and idempotent processing patterns.

5) Security and production ops (SASL/SSL, authentication, ACLs)
   - Why: moving to real clusters requires secure connections.
   - Tasks: test SSL/SASL connections locally or in staging, and read Kafka broker security docs.

6) Monitoring and observability (metrics, consumer lag, Dead Letter Queues)
   - Why: detect issues in production and handle poison messages.
   - Tasks: expose metrics (Micrometer), use Kafka consumer lag tools, implement DLQ for failed messages.

7) Message size and batching considerations
   - Why: large payloads (entire fetched pages) can exceed broker limits or be inefficient.
   - Tasks: consider truncation, compression, or storing large payloads externally and sending pointers in messages.

8) Transactions and exactly-once semantics (if needed)
   - Why: for some workflows you need atomic produce-consume sequences or idempotence guarantees.
   - Tasks: read about Kafka transactions and configure producer properties for idempotence.

## Suggested next code tasks (small experiments)
- Add a JSON serializer/deserializer and publish the ResponseCreater as JSON. Verify consumer deserializes into the DTO.
- Add a timeout and retry policy around RestTemplate calls and fail the request with meaningful HTTP response codes on repeated failures.
- Write one integration test using Embedded Kafka that posts to /api/streamLogs and asserts the listener logged/processed the message.

---

If you'd like, I created this summary as WIL.md at the repository root so you can keep iterating. I can also:
1) open a follow-up issue listing the next learning items (with checkboxes),
2) add an integration test skeleton, or
3) convert ResponseCreater to JSON serialization and update consumer code.

Tell me which of the three you'd like me to do next.