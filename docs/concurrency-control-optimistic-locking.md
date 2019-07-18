---
id: concurrency-control-optimistic-locking
title: Concurrency Control (Optimistic Locking)
---

Waltz implements a concurrency control using an optimistic locking. Instead of acquiring an exclusive lock on a resource explicitly, an optimistic locking verifies that there is no other transaction has modified the same resource during the transaction execution. Waltz uses the same concept, but it is made much lighter weight by taking advantage of log’s high-water mark. 

In Waltz a granularity of lock is not a record but an application defined lock ID. A lock ID consists of a name and a long integer ID. Waltz does not know what a lock ID represents. Waltz supports read locks and write locks. Multiple lock IDs may be associated with a single record. 

In many systems an optimistic locking is done by versioning records. When a record is updated, the version number of the record read by the updater is compared to the record in the database. If they match, it means there was no other transaction modified the same record, thus the update succeeds. If not, the update fails since the record has already been modified by other transactions. 

It can be very expensive to maintain the version numbers of all possible lock IDs. Instead, Waltz uses a probabilistic approach, similar to Bloom Filter, to keep the memory consumption predictable while achieving a low false negative rate. It uses hash values of lock IDs and the partition’s high-water mark. 

The data structure is a fixed size array. Waltz server maintains the array A of high-water marks with size L. We have N independent hash functions _hash-i (i = 0 .. N-1)_. The estimated high-water mark of lock ID is min { h | h = A[hash-i(id)], i = 0 .. N-1 }. If the estimate is smaller than the transaction’s high-water mark, the transaction is safe. There will be no false-negative. The false positive rate depends on the load. If we allocate a large array, we can keep the false negative rate reasonably low.

The above array is called a lock table. Lock tables are maintained by Waltz Server. There is one lock table for every partition. It means that the scope of a lock ID is limited to the partition. Therefore, a partitioning scheme must be decided taking a lock ID scope into consideration.

A client does not need to remember high-water mark for each lock ID. It only has to maintain the current client high-water mark, which is required for streaming anyways.

The optimistic lock is checked as follows.

1. The client remembers the client high-water mark at the beginning of transaction (just before invocation of the `execute` method of `TransactionContext`)
2. The client computes a transaction (the `execute` method of `TransactionContext`)
3. The client sends the transaction data and the high-water mark to Waltz server
4. Waltz server estimates the high-water mark for the lock IDs sent along with the transaction data using the lock table.
5. If the client’s high-water mark is higher than the estimated high-water mark, the client was up to date when the transaction is computed, thus success.
6. Otherwise, failure.
