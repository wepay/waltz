---
id: client-server-communication
title: Client-Server Communication
---

Client-Server communication uses persistent TCP connections. A client creates two connections per-server. One is for streaming, and the other for RPC. The networking module is built on top of Netty.

## Request ID (ReqId)

The request ID is a unique ID attached to a request message and corresponding response messages.

| Field | Data Type | Description |
|-------|-----------|-------------|
| Client ID | int | The unique ID of the client. The uniqueness is guaranteed by ZK.|
| Generation | int | The generation numbe of the partition. |
| Partition ID | int | The partition ID |
| Sequence number | int | The sequence number. |

## Mounting a partition

A client establishes a communication to servers in the following manner for each partition.

1. The client finds a server to which the partition is assigned.
2. The client sends a mount request to the server.
3. If the server has the partition,
	1. The server starts the transaction feed. The client accepts the feed data.
	2. If the feed reached the client high water mark, it sends the mount response with  partitionReady = true.
	3. The client receives the mount response and completes the mounting process.
4. Otherwise,
	1. The server sends the mount response with partitionReady = false.
	2. The client receives the mount response and find that the partition is not ready.
	3. Repeat from 1.

### Mount Request

| Field | Data Type | Description |
|-------|-----------|-------------|
| Request ID | ReqId | Client generated unique request ID |
| Client High-water Mark | long | The highest ID of transactions applied to the client application’s database |
| sequence number | int | A sequential ID that identifies the network client sending this request. This is used to detect stale network clients. |

### Mount Response

| Field | Data Type | Description |
|-------|-----------|-------------|
| Request ID | ReqId | Client generated unique request ID stored in the corresponding mount request
| Partition Ready | boolean | True if the partition is ready, otherwise false 

## Writing and Reading Transactions

### Append Request

The append request submits a transaction to Waltz.

| Field | Data Type | Description |
|-------|-----------|-------------|
| Request ID | ReqId | Client generated unique request ID |
| Client High-water Mark | long | The highest ID of transactions applied to the client application’s database |
| Write Lock Request Length | int | The length of the hash value array |
| Write Lock Request Hash Values | int[] | Lock hash values |
| Read Lock Request Length | int | The length of the hash value array |
| Read Lock Request Hash Values | int[] | Lock hash values |
| Transaction Header | int | Application defined 32-bit integer metadata |
| Transaction Data Length | int | The length of transaction data byte array. |
| Transaction Bytes | byte[] | A byte array |
| Checksum | int | CRC-32 of the transaction data |

### Feed Request

The feed request initiates the transaction feed from Waltz server. 

| Field | Data Type | Description |
|-------|-----------|-------------|
| Request ID | ReqId | Client generated unique request ID |
| Client High-water Mark | long | The highest ID of transactions applied to the client application’s database |

### Feed Data

The feed data is stream to a client in response to the feed request.

| Field | Data Type | Description |
|-------|-----------|-------------|
| Request ID | ReqId | Client generated unique request ID stored in the corresponding Feed Request. |
| Transaction ID | long | The ID of the transaction |
| Transaction Header | int | Application defined 32-bit integer metadata |

### Transaction Data Request

A transaction data request is sent through as a RPC request.

| Field | Data Type | Description |
|-------|-----------|-------------|
| Request ID | ReqId | Client generated unique request ID
| Transaction ID | long | The ID of the transaction to fetch

### Transaction Data Response

A transaction data response is sent through as a RPC response.

| Field | Data Type | Description |
|-------|-----------|-------------|
| Request ID | ReqId | Client generated unique request ID stored in the corresponding Feed Request. |
| Transaction ID | long | The ID of the transaction fetched |
| Success Flag | boolean | True if the fetch is successful, otherwise false. |
| Transaction Data Length | int | The length of transaction data. (exists only when the success flag is true) |
| Transaction Data Bytes | byte[] | A byte array (exists only when the success flag is true) |
| Checksum | int | CRC-32 of the transaction data (exists only when the success flag is true) |
| Error Message | String | Error message (exists only when the success flag is false) |

## Other Messages

### Flush Request

A client sends a flush request to wait for all pending transaction to complete regardless of successfully or not. A flush response will be sent back to the client when all append requests reached to the server before this request were completed.

| Field | Data Type | Description |
|-------|-----------|-------------|
| Request ID | ReqId | Client generated unique request ID |

### Flush Response

| Field | Data Type | Description |
|-------|-----------|-------------|
| Request ID | ReqId | Client generated unique request ID stored in the corresponding Flush Request. |
| Transaction ID | long | The high-water mark after pending transactions are processed. |

### Lock Failure

A lock failure message is sent back to a client when a lock request failed.

| Field | Data Type | Description |
|-------|-----------|-------------|
| Request ID | ReqId | Client generated unique request ID stored in the corresponding Append Request |
| Transaction ID | long | The ID of the transaction that made the lock request fail. |

## Generation Number

The generation number is used to ensure that Waltz server instances and clients work consistently in the dynamically changing environment.

Waltz Server cluster consists one or more Waltz server instances. A partition is assigned to a single server instance at any moment. The cluster manager is responsible for the assignments. When a new instance comes up, or an old instance goes down, the cluster manager detects it and reassign partitions to make certain that there is one and only one instance for each partition. Everytime this happens, the cluster manager bumps up the partition’s generation number. Waltz servers ignores any append request when the generation number does not match.

## Detection of Failed Append Requests

Each client has a registry of append requests called the transaction monitor. The transaction monitor determines a state (success/failure) of each append request using the transaction feed.

For each transaction in the feed,

1. The client checks if its transaction monitor contains the ReqId in the feed data.
2. If the transaction monitor has the ReqId, the transaction was issued by this client and successful, so,
	1. The transaction monitor marks the transaction as success.
	2. The transaction monitor marks any pending transactions older than this transaction as failure.
	3. The transaction monitor clears the entries of completed (either success or failure) transaction in the registry
3. Otherwise, ignore

A client automatically reconnects to Waltz servers after a network failure or a Waltz server failure. When a reconnect happens, it is possible that the server may have lost some requests. It may take a while for the client to recognize it especially for a service generating append requests in a slow pace. To ensure a quicker detection, the following reconnect procedure has designed.

1. The client blocks append requests
2. The client establishes a connection to the server
3. The client send a mount request
4. The client starts receiving transaction feed and apply the regular failure detection
5. The client makes all pending requests fail when the mount response is received
6. The client unblocks append requests

The completion of a mount request ensures that the server does not have any pending request from this client. The client can safely get rid of pending requests as failures.
