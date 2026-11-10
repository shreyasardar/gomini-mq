# DistributedQueue — A Persistent Message Broker in Go

A lightweight message broker built in Go, featuring topic-based messaging, persistent storage, message recovery, and acknowledgment-based delivery.

## Overview

DistributedQueue is a backend messaging system designed to demonstrate the core concepts behind message brokers and asynchronous communication systems.

It allows producers to publish messages to topics and consumers to retrieve and process them through a REST API.

Unlike a purely in-memory queue, DistributedQueue persists messages to disk and recovers unacknowledged messages after application restarts.

## Features

- **Topic-based messaging:** Organize messages into independent topics.
- **Message publishing:** Publish messages through HTTP REST APIs.
- **Persistent storage:** Store messages in append-only JSONL files.
- **Message recovery:** Restore persisted messages when the broker restarts.
- **Delivery tracking:** Move delivered messages into an in-flight state.
- **Acknowledgment (ACK):** Persist successful message acknowledgments.
- **Negative acknowledgment (NACK):** Return unsuccessfully processed messages to the ready queue.
- **Crash recovery:** Redeliver messages that were not acknowledged before shutdown.
- **Concurrent access:** Protect queue, broker, and storage operations using synchronization primitives.
- **Graceful shutdown:** Shut down the HTTP server and close broker resources cleanly.
- **Automated testing:** Test queue operations, persistence, recovery, and concurrency.

## Architecture

```text
                 Producer
                    |
                    | POST /publish
                    v
              +-------------+
              |  Gin REST   |
              |     API     |
              +-------------+
                    |
                    v
              +-------------+
              |   Broker    |
              +-------------+
                    |
                    v
              +-------------+
              |   Topics    |
              +-------------+
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

### Message lifecycle

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
- curl (for API testing)

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

The HTTP server starts on:

```text
http://localhost:8080
```

The broker loads existing topic logs from the `data/` directory during startup.

**Note:** Topics must be provisioned through the broker's `CreateTopic` method before publishing. The current REST API does not expose a topic-creation endpoint.

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

### 6. Legacy Consume

**GET `/consume?topic=orders`**

Removes and returns the next ready message without acknowledgment.

```bash
curl "http://localhost:8080/consume?topic=orders"
```

This is a legacy operation. Use `/deliver` and `/ack` for acknowledgment-based processing.

## Persistence and Recovery

DistributedQueue uses append-only JSONL files for storing messages.

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

## Concurrency and Synchronization

The project uses Go synchronization primitives to protect shared state.

- `sync.Mutex` protects queue operations and storage writes.
- `sync.RWMutex` protects broker topic access.
- A lifecycle lock prevents broker operations from racing with shutdown.
- ACK persistence happens before removing a message from the in-flight queue.

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

Run benchmarks:

```bash
go test -bench=. -benchmem ./...
```

The test suite covers:

- Message publishing and consumption.
- Concurrent queue operations.
- Message delivery and acknowledgment.
- NACK and redelivery.
- Storage append and recovery.
- ACK persistence.
- Broker recovery after restart.
- Graceful shutdown.

## Design Decisions

**Why append-only logs?**

Append-only storage simplifies persistence by writing new records sequentially instead of modifying existing records.

**Why separate ACK logs?**

Separating message records from acknowledgment records keeps message serialization simple and allows recovery to identify acknowledged messages independently.

**Why in-flight messages?**

A delivered message should not be permanently removed until the consumer confirms successful processing. In-flight tracking makes ACK and NACK possible.

**Why recover unacknowledged messages?**

If the application crashes after delivery but before acknowledgment, the message should become available again rather than being silently lost.

## Current Limitations

- Storage uses local disk rather than a distributed storage system.
- The queue and in-flight state are maintained in memory.
- ACK records are synced individually, which can affect throughput.
- There is no consumer-group coordination or partitioning.
- There is no automatic retry limit or dead-letter queue.
- Topic creation is currently available through the broker API, not an HTTP endpoint.
- Message log compaction and retention are not implemented.

## Future Improvements

- Consumer groups and multiple consumers.
- Automatic retry policies.
- Dead-letter queues.
- Message retention and log compaction.
- Topic creation and management APIs.
- Batch publishing and acknowledgment.
- Metrics and monitoring dashboard.
- Distributed broker replication.

## Author

**Shreya Sardar**

B.Tech — Artificial Intelligence