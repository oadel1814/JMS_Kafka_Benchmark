# JMS vs Kafka — Messaging Systems Benchmark Project

A personal benchmarking and research project comparing **JMS (ActiveMQ)** and **Apache Kafka** in terms of:

- Performance
- Throughput
- Latency
- Developer experience
- Ecosystem integrations
- Real-world suitability for data-intensive systems

---

# Project Goals

The goal of this project was to explore the architectural and practical differences between traditional enterprise messaging systems and modern distributed event streaming platforms.

The project focuses on answering questions such as:

- Which system provides lower latency?
- Which system scales better under heavy load?
- Which is easier to integrate into modern data pipelines?
- What trade-offs exist between simplicity and throughput?

---

# Technologies Used

## JMS Stack
- Apache ActiveMQ
- Jakarta JMS API
- Java

## Kafka Stack
- Apache Kafka 3.7
- Kafka Producer / Consumer APIs
- Kafka performance testing tools
- Java

---

# Benchmarks Implemented

The project includes custom benchmarking tools written in Java to evaluate:

1. Produce Response Time
2. Consume Response Time
3. Maximum Produce Throughput
4. Maximum Consume Throughput
5. End-to-End Message Latency

---

# Benchmark Methodology

## Produce Response Time

Measures the time required for a producer to send a single message and receive acknowledgment from the broker.

### JMS
Used synchronous:

```java
producer.send(message);
```

### Kafka
Used:

```java
producer.send(record).get();
```

to force synchronous acknowledgment.

Median measured over 1000 messages.

---

## Consume Response Time

Measures the time required for a consumer to receive messages from the broker.

Kafka measurements used polling:

```java
consumer.poll(Duration.ofSeconds(5));
```

while JMS used blocking receives:

```java
consumer.receive(5000);
```

---

## Maximum Throughput

Measures the highest sustainable messages-per-second rate before failures occur.

An exponential search strategy was used:

```text
100 → 200 → 400 → 800 → ...
```

until delivery failures or timing instability appeared.

Kafka throughput was additionally validated using:

```bash
kafka-producer-perf-test.sh
kafka-consumer-perf-test.sh
```

---

## End-to-End Latency

Latency was measured by embedding timestamps into messages.

Example:

```text
timestamp|payload
```

Consumer-side latency:

```text
receiveTime - sendTime
```

Median latency measured over 10,000 messages.

A `CountDownLatch` was used to ensure the consumer was fully connected before producers began sending.

---

# Results

| Metric | JMS (ActiveMQ) | Kafka | Winner |
|---|---|---|---|
| Produce Response Time | 0.039 ms | 0.250 ms | JMS |
| Consume Response Time | 0.038 ms | 0.063 ms | JMS |
| Max Produce Throughput | 51,200 msg/s | 206,697 msg/s | Kafka |
| Max Consume Throughput | 51,200 msg/s | 224,317 msg/s | Kafka |
| Median Latency | 2 ms | 1 ms | Kafka |

---

# Key Findings

## JMS Strengths

- Extremely low single-message response time
- Simple enterprise-style API
- Easy setup and monitoring
- Great integration with Spring and Java EE ecosystems

## JMS Weaknesses

- Lower throughput
- Java-centric ecosystem
- No message replay capability
- Limited native big-data integrations

---

## Kafka Strengths

- Extremely high throughput
- Persistent log-based architecture
- Message replay support
- Strong big-data ecosystem integrations
- Native stream processing support
- Multi-language client ecosystem

## Kafka Weaknesses

- More operational complexity
- Consumer group coordination overhead
- Higher per-message response time
- More difficult initial setup

---

# Technical Challenges Encountered

## 1. Incorrect Latency Measurements

Early measurements produced latencies in the millions of milliseconds due to:

- Consumers reading old retained messages
- Consumer groups starting at old offsets
- Producer starting before consumer rebalance completed

### Solution

Implemented:
- Unique benchmark run IDs
- Fresh consumer groups
- `CountDownLatch`
- Assignment synchronization

---

## 2. Throughput Timing Inaccuracy

At very high throughput values, Linux thread sleep resolution became insufficient.

Example:

```text
Thread.sleep(<1ms)
```

became effectively unreliable.

### Solution

- Added elapsed-time validation
- Cross-validated results with Kafka’s official performance tools

---

## 3. Kafka Polling Semantics

Kafka consumers require active polling and group coordination.

This introduced complexities such as:
- rebalance waiting
- assignment synchronization
- offset handling
- timeout handling

---

# Architectural Observations

## JMS Architecture

JMS behaves more like:
- traditional enterprise messaging
- request/reply middleware
- transient delivery system

Optimized for:
- simplicity
- transactional workflows
- business applications

---

## Kafka Architecture

Kafka behaves more like:
- a distributed append-only log
- an event streaming platform
- a persistent data pipeline

Optimized for:
- scale
- analytics
- streaming
- distributed systems

---

# Final Conclusion

For modern data-intensive systems, Kafka is the stronger platform due to:

- significantly higher throughput
- replayable event logs
- native stream processing
- stronger integration ecosystem
- better scalability characteristics

However, JMS/ActiveMQ still remains an excellent choice for:
- smaller enterprise systems
- Java-centric applications
- low-latency request/reply messaging
- simpler operational environments

The project demonstrates that the "best" messaging system depends heavily on:
- workload characteristics
- scalability requirements
- operational complexity tolerance
- integration needs

---

# Future Improvements

Potential future extensions for this project:

- Multi-broker Kafka cluster testing
- Replication-factor benchmarking
- Persistent vs non-persistent JMS delivery comparison
- Compression benchmarking
- Batch-size optimization
- Kafka Streams benchmarking
- Dockerized benchmark environment
- Grafana + Prometheus monitoring dashboards

---

# Repository Structure

```text
/src
 ├── KafkaBenchmark.java
 ├── JMSBenchmark.java
 ├── resources/
 │    └── message.txt
 └── benchmark-results/
```

---

# References

- Apache Kafka Documentation
- Apache ActiveMQ Documentation
- Confluent Platform Docs
- Kafka Connect Documentation
- Jakarta JMS Specification
