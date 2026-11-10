# DistributedQueue — Persistent Message Broker in Go

A lightweight, topic-based message broker built in Go, featuring persistent storage, concurrent message processing, acknowledgment-based delivery, and crash recovery.

[![Go](https://img.shields.io/badge/Go-1.26-blue)](https://go.dev/)
[![Gin](https://img.shields.io/badge/Framework-Gin-green)](https://gin-gonic.com/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Overview

DistributedQueue is a backend messaging system designed to explore the core concepts behind message brokers and asynchronous communication systems.

It enables producers to publish messages to topics and consumers to retrieve and process them through REST APIs.

Unlike a purely in-memory queue, DistributedQueue persists messages to disk and recovers unacknowledged messages after application restarts.

The project focuses on reliable message delivery, concurrent processing, persistent storage, and recovery mechanisms.

## Features

- **Topic-based messaging:** Organize messages into independent topics.
- **Message publishing:** Publish messages through HTTP REST APIs.
- **Persistent storage:** Store messages in append-only JSONL files.
- **Message recovery:** Restore persisted messages when the broker restarts.
- **Delivery tracking:** Move delivered messages into an in-flight state.
- **Acknowledgment (ACK):** Persist successful message acknowledgments before removing messages from in-flight memory.
- **Negative acknowledgment (NACK):** Return unsuccessfully processed messages to the ready queue.
- **Crash recovery:** Redeliver messages that were not acknowledged before a restart.
- **Concurrent access:** Protect shared queue, broker, and storage operations using synchronization primitives.
- **Graceful shutdown:** Shut down the HTTP server and close broker resources cleanly.
- **Automated testing:** Test queue operations, persistence, recovery, and concurrency.

## Architecture

```text
                  Producer
                     |
                     | POST /publish
                     v
              +--------------+
              |   Gin REST   |
              |      API     |
              +--------------+
                     |
                     v
              +--------------+
              |    Broker    |
              +--------------+
                     |
                     v
              +--------------+
              |    Topics    |
              +--------------+
                     |
              +------+------+
              |             |
              v             v
        +-----------+  +-----------+
        |   Queue   |  |  Storage  |
        |           |  |           |
        | Ready     |  | Message   |
        | In-flight |  | ACK logs  |
        +-----------+  +-----------+
              |
              v
           Consumer
```

### Message Lifecycle

```text
Producer
   |
   v
Publish
   |
   v
Persistent Message Log
   |
   v
Ready Queue
   |
   v
Deliver
   |
   v
In-flight Queue
   |
   +-----------> ACK
   |               |
   |               v
   |          Persist ACK
   |               |
   |               v
   |          Remove message
   |
   +-----------> NACK
   |               |
   |               v
   |          Return to ready queue
   |
   +-----------> Application restart
                   |
                   v
             Recover unACKed
                messages
```

## Project Structure

```text
gominimq/
|
|-- api/
|   |-- handler.go
|   |-- api_test.go
|
|-- broker/
|   |-- broker.go
|   |-- broker_test.go
|   |-- broker_throughput_test.go
|   |-- ack_recovery_test.go
|
|-- models/
|   |-- message.go
|
|-- queue/
|   |-- queue.go
|   |-- queue_test.go
|
|-- storage/
|   |-- log.go
|   |-- ack_log.go
|   |-- log_test.go
|   |-- ack_log_test.go
|
|-- topic/
|   |-- topic.go
|   |-- topic_test.go
|
|-- main.go
|-- go.mod
|-- go.sum
|-- README.md
```

## Technology Stack

| Technology | Purpose |
|---|---|
| Go | Core implementation |
| Gin | HTTP REST API |
| JSON | Message serialization |
| JSONL | Append-only persistent logs |
| Mutex / RWMutex | Concurrent access protection |
| Go testing | Unit and integration testing |

## Getting Started

### Prerequisites

- Go 1.26 or compatible version
- Git
- curl

### Clone the repository

```bash
git clone https://github.com/shreyasardar/gomini-mq.git
cd gomini-mq
```

### Install dependencies

```bash
go mod download
```

### Run the application

```bash
go run main.go
```

The HTTP server starts at:

```text
http://localhost:8080
```

The broker loads existing topic logs from the `data/` directory during startup.

**Note:** Topics must be created through the broker's `CreateTopic` method before publishing. The current REST API does not expose a topic-creation endpoint.

## REST API

### 1. Health Check

**GET `/ping`**

```bash
curl http://localhost:8080/ping
```

Response:

```json
{
  "message": "pong"
}
```

### 2. Publish a Message

**POST `/publish`**

```bash
curl -X POST http://localhost:8080/publish \
-H "Content-Type: application/json" \
-d '{
  "topic": "orders",
  "key": "order-101",
  "value": "Laptop order"
}'
```

Response:

```json
{
  "message": "Message published successfully"
}
```

### 3. Deliver a Message

**POST `/deliver`**

Moves the next available message from the ready queue into the in-flight state.

```bash
curl -X POST http://localhost:8080/deliver \
-H "Content-Type: application/json" \
-d '{
  "topic": "orders"
}'
```

Example response:

```json
{
  "ID": 1,
  "Key": "order-101",
  "Value": "Laptop order",
  "Timestamp": 1794267009
}
```

### 4. Acknowledge a Message

**POST `/ack`**

Confirms successful message processing and persists the acknowledgment.

```bash
curl -X POST http://localhost:8080/ack \
-H "Content-Type: application/json" \
-d '{
  "topic": "orders",
  "message_id": 1
}'
```

Response:

```json
{
  "message": "Message acknowledged successfully"
}
```

### 5. Negative Acknowledgment

**POST `/nack`**

Returns an unsuccessfully processed message to the ready queue.

```bash
curl -X POST http://localhost:8080/nack \
-H "Content-Type: application/json" \
-d '{
  "topic": "orders",
  "message_id": 1
}'
```

Response:

```json
{
  "message": "Message returned to queue"
}
```

### 6. Legacy Consume

**GET `/consume?topic=orders`**

Removes and returns the next ready message without acknowledgment.

```bash
curl "http://localhost:8080/consume?topic=orders"
```

This is a legacy operation. Use `/deliver` and `/ack` for acknowledgment-based processing.

## Persistence and Recovery

DistributedQueue uses append-only JSONL files to persist messages.

Each topic has two associated files:

```text
data/
|
|-- orders.log
|-- orders.ack
|
|-- payments.log
|-- payments.ack
```

### Message Log

The `.log` file stores published messages.

Example:

```json
{"ID":1,"Key":"order-101","Value":"Laptop","Timestamp":123456}
{"ID":2,"Key":"order-102","Value":"Phone","Timestamp":123457}
```

### ACK Log

The `.ack` file stores acknowledged message IDs.

Example:

```text
1
2
```

### Recovery Process

When the broker starts:

1. Discover existing topic message logs.
2. Read persisted messages.
3. Read acknowledged message IDs.
4. Exclude acknowledged messages.
5. Restore unacknowledged messages into the ready queue.
6. Restore the next message ID based on persisted message IDs.

This ensures that messages delivered but not acknowledged before a restart become available again.

### ACK Durability

The ACK operation follows this sequence:

1. Verify that the message is in-flight.
2. Append its ID to the ACK log.
3. Synchronize the ACK log to disk.
4. Remove the message from the in-flight queue.

If ACK persistence fails, the message remains in-flight.

## Concurrency and Synchronization

The project uses Go synchronization primitives to protect shared state.

- `sync.Mutex` protects queue operations and storage writes.
- `sync.RWMutex` protects broker topic access.
- A lifecycle lock prevents broker operations from racing with shutdown.
- ACK persistence happens before removing a message from the in-flight queue.

## Performance Benchmarks

DistributedQueue was evaluated using concurrent producers publishing messages to a disk-backed append-only JSONL log.

### Benchmark Configuration

| Parameter | Configuration |
|---|---|
| Workload | Concurrent message publishing |
| Storage | Append-only JSONL |
| Producers | 1, 10, 50, 100 |
| Messages per producer | 1,000 |
| Maximum total messages | 100,000 |
| Failed publishes | 0 |

### Benchmark Results

| Producers | Total Messages | Execution Time | Throughput |
|---:|---:|---:|---:|
| 1 | 1,000 | 0.699 ms | 1.43M msg/s |
| 10 | 10,000 | 9.319 ms | 1.07M msg/s |
| 50 | 50,000 | 47.402 ms | 1.05M msg/s |
| 100 | 100,000 | 97.154 ms | 1.03M msg/s |

<details>
<summary>Detailed benchmark output</summary>

```text
Producers: 1
Total messages: 1000
Execution time: 699.005µs
Throughput: 1430604.93 messages/sec
Failed publishes: 0

Producers: 10
Total messages: 10000
Execution time: 9.319162ms
Throughput: 1073057.86 messages/sec
Failed publishes: 0

Producers: 50
Total messages: 50000
Execution time: 47.401926ms
Throughput: 1054809.46 messages/sec
Failed publishes: 0

Producers: 100
Total messages: 100000
Execution time: 97.153802ms
Throughput: 1029295.80 messages/sec
Failed publishes: 0
```

</details>

### Observations

- Throughput remained above one million publish operations per second across all four tested configurations.
- The 100-producer experiment processed 100,000 messages in approximately 97 ms.
- All four experiments completed with zero failed publish operations.
- Throughput decreased moderately as producer concurrency increased.

### Running the Benchmark

```bash
go test -run TestPublishThroughput -v ./broker
```

**Benchmark notes:**

These results are from a local, single-run experiment. Message writes use append operations without an individual `fsync` for every message. ACK writes use synchronization to disk. Results depend on hardware, operating system, filesystem, and workload.

These measurements should not be interpreted as production throughput guarantees. Repeated benchmark runs and median measurements are recommended for more reliable comparisons.

## Testing

Run all tests:

```bash
go test ./...
```

Run tests with the race detector:

```bash
go test -race -count=1 ./...
```

Run static analysis:

```bash
go vet ./...
```

Run Go benchmarks:

```bash
go test -bench=. -benchmem ./...
```

### Test Coverage Areas

- Message publishing and consumption.
- Concurrent queue operations.
- Message delivery and acknowledgment.
- NACK and redelivery.
- Storage append and recovery.
- ACK persistence.
- Broker recovery after restart.
- Graceful shutdown.
- Concurrent access and race detection.

## Design Decisions

### Why append-only logs?

Append-only storage simplifies persistence by writing new records sequentially instead of modifying existing records.

### Why separate ACK logs?

Separating message records from acknowledgment records keeps message serialization simple and allows recovery to identify acknowledged messages independently.

### Why in-flight messages?

A delivered message should not be permanently removed until the consumer confirms successful processing. In-flight tracking makes ACK and NACK possible.

### Why recover unacknowledged messages?

If the application crashes after delivery but before acknowledgment, the message should become available again rather than being silently lost.

## Current Limitations

- Storage uses local disk rather than a distributed storage system.
- The queue and in-flight state are maintained in memory.
- ACK records are synced individually, which can affect throughput.
- There is no consumer-group coordination or partitioning.
- There is no automatic retry limit or dead-letter queue.
- Topic creation is currently available through the broker API, not an HTTP endpoint.
- Message log compaction and retention are not implemented.
- Message delivery is at-least-once in the recovery scenario, so consumers should account for possible duplicate processing.

## Future Improvements

- Consumer groups and multiple consumers.
- Automatic retry policies.
- Dead-letter queues.
- Message retention and log compaction.
- Topic creation and management APIs.
- Batch publishing and acknowledgment.
- Metrics and monitoring dashboard.
- Distributed broker replication.
- Repeated benchmark runs and latency measurements.

## Author

**Shreya Sardar**

B.Tech — Artificial Intelligence and Data Science  
IIT Jodhpur

GitHub: [shreyasardar](https://github.com/shreyasardar)

Repository: [DistributedQueue](https://github.com/shreyasardar/gomini-mq)

---

*DistributedQueue — Exploring message brokers, persistence, concurrency, and reliable message delivery using Go.*
