# System Design Comprehensive Cheatsheet

---

## 1. Core Framework & Interview Blueprint

A structured approach prevents architectural gaps and keeps discussions grounded.

```
+-------------------------------------------------------------+
| 1. Requirements Clarification (Functional & Non-Functional) |
+-------------------------------------------------------------+
                              |
                              v
+-------------------------------------------------------------+
| 2. Capacity Estimations (Traffic, Storage, Bandwidth)       |
+-------------------------------------------------------------+
                              |
                              v
+-------------------------------------------------------------+
| 3. High-Level Architecture (API Contracts & Core Components)|
+-------------------------------------------------------------+
                              |
                              v
+-------------------------------------------------------------+
| 4. Data Modeling (Schema, Access Patterns, Storage Engines) |
+-------------------------------------------------------------+
                              |
                              v
+-------------------------------------------------------------+
| 5. Detailed Component Design & Scaling Bottlenecks          |
+-------------------------------------------------------------+

```

---

## 2. Fundamental Architectural Trade-offs

### CAP Theorem & PACELC

- **CAP Theorem:** In the presence of a network partition (**P**), a distributed system must choose between:
- **Consistency (CP):** Every read receives the most recent write or an error (e.g., etcd, ZooKeeper, CockroachDB).
- **Availability (AP):** Every non-failing node returns a response, but it may contain stale data (e.g., Cassandra, DynamoDB).

- **PACELC Extension:** If there is a Partition (**P**), trade off Availability (**A**) vs Consistency (**C**); **E**lse, trade off Latency (**L**) vs Consistency (**C**).

### SQL vs. NoSQL Engine Selection

| Factor          | SQL (Relational)                                         | NoSQL (Document / Wide-Column / Key-Value)            |
| --------------- | -------------------------------------------------------- | ----------------------------------------------------- |
| **Data Schema** | Fixed, rigid schema with ACID transactions               | Dynamic, schema-less, eventual consistency (BASE)     |
| **Scaling**     | Vertical scale-up; horizontal via read replicas/sharding | Native horizontal partitioning (auto-sharding)        |
| **Query Type**  | Complex joins, relational aggregation                    | Key-based lookups, denormalized read-heavy operations |
| **Examples**    | PostgreSQL, MySQL                                        | MongoDB, Cassandra, DynamoDB, Redis                   |

---

## 3. Scalability & System Components

### Load Balancing

- **Layer 4 (Transport):** Direct packet routing based on IP and TCP/UDP ports without payload inspection (e.g., AWS NLB, HAProxy).
- **Layer 7 (Application):** Routes requests based on HTTP headers, cookies, and path content (e.g., NGINX, AWS ALB, Envoy).
- **Algorithms:** Round Robin, Least Connections, Weighted Least Response Time, IP Hash.

### Consistent Hashing

- **The Problem:** Modulo hashing (`hash(key) % N`) forces remapping almost all keys whenever server count $N$ changes.
- **The Solution:** Arrange server nodes on a virtual ring (0 to $2^{32}-1$). Keys map to the next clockwise server.
- **Virtual Nodes (VNodes):** Maps a single physical server to multiple positions across the ring to prevent data skew and hotspots.

### Caching Layers & Write Patterns

- **Eviction Policies:** LRU (Least Recently Used), LFU (Least Frequently Used), FIFO.

| Pattern                 | How It Works                                                        | Trade-offs                                                  |
| ----------------------- | ------------------------------------------------------------------- | ----------------------------------------------------------- |
| **Cache-Aside**         | App reads cache; on miss, reads DB and populates cache              | Stale data window; handles cache failures gracefully        |
| **Read-Through**        | App treats cache as primary data source; cache fetches missing data | Cleaner app code; cache cold-starts cause latency           |
| **Write-Through**       | Writes update cache and DB synchronously                            | High write latency; eliminates data staleness               |
| **Write-Back (Behind)** | Writes hit cache first; async flush to DB in batches                | Lowest latency, high throughput; risk of data loss on crash |

---

## 4. Asynchronous Processing & Messaging

```
[ Client ]
    │
    ▼
[ API Gateway ]
    │
    ▼
[ Producer Service ] ──(Publishes)──► [ Message Broker / Event Log ]
                                              │
                     ┌────────────────────────┴────────────────────────┐
                     ▼                                                 ▼
          [ Worker Service A ]                              [ Worker Service B ]
          (Order Fulfillment)                               (Notification Engine)

```

- **Message Queues (RabbitMQ, SQS):** Transient point-to-point delivery. Messages are acknowledged and removed upon consumption.
- **Event Streaming Logs (Kafka, Kinesis):** Append-only distributed commit logs with durable, replayable offsets. Multiple consumer groups independently track read positions.
- **Delivery Guarantees:** At-most-once, At-least-once (requires consumer **idempotency** via unique request UUIDs), Exactly-once (requires distributed coordination/transactional outbox).

---

## 5. Storage Sharding & Replication

- **Horizontal Sharding Strategies:**
- **Range-Based:** Easy range scans, but prone to hotspots (e.g., date-based tables).
- **Hash-Based:** Even data distribution across partitions, but broad range scans require cross-shard scattering.
- **Directory/Lookup-Based:** Flexible routing via mapping table; introduces an extra network hop and routing service dependency.

- **Replication Topologies:**
- **Single-Leader:** All writes go to the primary node; followers consume replication logs for read scalability.
- **Multi-Leader:** Useful for multi-datacenter topologies; requires conflict resolution strategies (Last-Write-Wins or CRDTs).
- **Leaderless (Quorum):** Quorum reads and writes satisfy $R + W > N$ where $N$ is replica count, $R$ is read quorum, and $W$ is write quorum.

---

## 6. Real-World Cheat Sheet: Latency Numbers Every Engineer Should Know

```
L1 cache reference ......................... 0.5 ns
Branch mispredict .......................... 5   ns
L2 cache reference ......................... 7   ns
Mutex lock/unlock .......................... 25  ns
Main memory reference ...................... 100 ns
Compress 1KB with Zstandard ................ 2.0 µs
Send 2KB over 1 Gbps network ............... 20  µs
Read 1 MB sequentially from memory ......... 250 µs
Round trip within same datacenter .......... 500 µs
Read 1 MB sequentially from NVMe SSD ....... 1   ms
Disk seek (HDD) ............................ 10  ms
Read 1 MB sequentially from 1 Gbps network . 10  ms
Packet round trip cross-continent .......... 150 ms

```

---

## 7. High-Level Blueprint Checklist

When designing any system end-to-end, trace through these verification vectors:

- [ ] **Availability:** Redundant multi-zone compute, auto-scaling groups, health checks.
- [ ] **Fault Tolerance:** Circuit breakers, exponential backoff with jitter, dead-letter queues.
- [ ] **Data Integrity:** ACID boundaries, idempotency keys, dual-write protection with the Outbox pattern.
- [ ] **Security:** TLS termination, API gateway rate-limiting, least-privilege service-to-service IAM roles.
- [ ] **Observability:** Distributed tracing (OpenTelemetry), structured log aggregation, RED metrics (Rate, Errors, Duration).
