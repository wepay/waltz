---
id: introduction
title: Introduction
---

Waltz is a distributed/replicated write-ahead log. It aims to be a generic write-ahead log to help a micro-service architecture to perform reliable/consistent transactions in a distributed environment. Waltz uses a quorum for the guarantee of persistence and consistency. It also provides a machinery for concurrency control which is essential for a transaction system to prevent inconsistent transactions from being entered into the system. Waltz provides a single image of globally consistent transaction log to micro-services.

A basic usage pattern in micro-service architecture is the following. A micro-service creates a transaction data from a request from the outside world and its current state (database state). The transaction data is a packet that describes intended data updates. Then the micro-service sends it to Waltz before updating its database. Once the transaction is persisted in Waltz, the micro-service can safely updates its database. Even when a micro-service failed, it recovers its state from the Waltz log. Furthermore, the log can be consumed by other services to produce derived data such as a summary data and a report. They can produce consistent results independently from the main transaction system. It is expected that this will reduce direct dependencies among micro-services and makes the whole system more resilient to faults.

In addition, Waltz has a built-in concurrency control which provides a non-blocking optimistic locking. The granularity of a lock is controlled by applications and the lock must be explicitly specified in a transaction. This can be used for preventing two or more micro-service instances from writing inconsistent transaction data into the log. 

Waltz log is partitioned. Partitioning provides higher read/write throughput by reducing contentions. The partitioning scheme is customizable by applications.

All transaction data are replicated to multiple places synchronously. Waltz uses a quorum write system. As long as more than half of replicas are up, Waltz is available and guarantees consistency of data. Replicas may be placed in different data centers for disaster resilience.

In sum, Waltz provides globally consistent transaction information to applications in highly scalable/reliable ways. Waltz can be used as the source of truth in a large distributed system.
