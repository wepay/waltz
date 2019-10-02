---
id: waltz-client
title: Waltz Client
---

## Client-Application Interactions

An application must define a subclass of `WaltzClientCallback` and supply an instance of it to the constructor of `WaltzClient`. Through the callbacks, Waltz client gets the latest client high-water mark for a partition and also applies a committed transaction to the application.

![Waltz client](/img/docs/waltz-client.png)

An application must define a subclass of TransactionContext to send a transaction data to Waltz. A transaction context encapsulates an application logic and data that are necessary to generate a transaction data.

1. The application submits an instance of TransactionContext
2. The Waltz client calls the `getClientHighWaterMark` method of `WaltzClientCallbacks` to get the latest client high-water mark
3. The Waltz client constructs `TransactionBuilder` with the client high-water mark.
4. The Waltz client invokes the execute method of the context with the instance of `TransactionBuilder`.
5. The application code (the `execute` method of `TransactionContext`) builds a transaction data.
6. The Waltz client sends a append request to a Waltz server to write the transaction data.
7. The control returns to the application.
8. In the background, the Waltz client is monitoring the status of the append request.
    1. If the append request was not successful, the Waltz client puts the transaction context into the retry queue. This means that the commit order may be different from the order that transactions are submitted.
    2. The Waltz client executes the failed transaction context asynchronously.

Transaction data successfully appended to the log will be streamed back to the application eventually.

1. The Waltz client receives feeds of successful transactions. Feeds includes transaction IDs and transaction headers
2. The Walz client invokes the `applyTransaction` callback
3. The application code (the `applyTransaction` callback) process the transaction and updates the client high-water mark.

TransactionContext also gets notifications through callbacks.

* onCompletion(true) is called when the transaction if committed
* onCompletion(false) is called when the transaction failed, and no retry is scheduled.
* onException(exception) is called when the transaction failed with an exception. (No retry will be scheduled)

Although Waltz client does not provide a blocking behavior that is to block an application thread until the transaction is completed, the application can implement such a behavior using these methods.

## Client-Server Interactions

There are two kinds of interaction with servers, streaming and RPC. Streaming guarantees ordering of requests and responses (including transaction feeds). RPC does not guarantee or ordering. RPC is used for retrieving transaction data for a particular transaction ID. Everything else uses streaming.

`WaltzClient` has two internal clients, `StreamClient` (the implementation class is `InternalStreamClient`) and `RpcClient` (the implementation class is `InternalRpcClient`). They manages streaming connections and RPC connections, respectively. They have similar structures. Actually their implementation classes extend the common superclass `InternalBaseClient`.

### InternalBaseClient

`InternalBaseClient` manages connection to all servers the client is communicating. A single connection to a server is represented by an instance of `WaltzNetworkClient` in `InternalBaseClient`. An instance of `InternalBaseClient` has a network client map keyed by the endpoint (the server address).

`InternalBaseClient` has another map, a partition object map keyed by the partition ID. A partition object works like a proxy to the partition managed by a remove server. A partition object has an associated network client. This association is not permanent. It is updated accordingly when the partition assignment to servers are changed. `InternalBaseClient` calls the `mountPartition` method of the network client to tell it that it is now responsible for that partition. The network client, in turn, calls the `mounting` method of the partition object to tell the partition the network client is now responsible. If the network channel is ready, the network client calls back `InternalBaseClient`’s `onMountingPartition` method to finish mounting. If not, `onMountingPartition`  is called later when the channel becomes ready.

### InternalStreamClient

`InternalStreamClient` extends `InternalBaseClient`. The `onMountingPartition` method of `InternalStreamClient` sends a mount request to the server to set up a streaming context. Another important method is `onTransactionReceived`, which is invoked when the network client received a transaction from a server. This method invokes the `applyTransaciton` method of `WaltzClientCallbacks`.

### InternalRpcClient

`InternalRpcClient` extends `IntenalBaseClient`. The `onMountingPartition` method of `InternalRpcClient` does not send a mount request to the server because RpcClient does not require streaming context at all. Instead, it sends all pending `TransactionDataRequests` to the new server. `onTransactionReceived` is not supposed to be used. So, it just throws an exception.

### Partition

Both `InternalStreamClient` and `InternalRpcClient` have a map of partition objects. Each partition object has the following important data structures

* `TransactionMonitor`
* A map of Futures keyed by transaction ID
* `WaltzNetworkClient`

`TransactionMonitor` is used by `InternalStreamClient` to keep track of states of append requests. The map of future is used by `RpcClient` to keep track of pending transaction data requests. `WaltzNetworkClient` is a network client responsible for communication with the server for this partition.

### Fault Recovery

A network client is closed whenever an unexpected error occurs. We basically don’t try to recover using the same connection. A new network client is created immediately to recover a connection and perform the partition mounting process again. Each network client has a sequence number incremented on every creation. The stream client send the mount request includes the sequence number of the network client so that server can discard all messages from old connections after it receives the mount request. This process is exactly same as handling of partition assignment change.
