---
id: application-programming-model
title: Application Programming Model
---

## Basic Idea

An application should use Waltz as a write-ahead log. It should write to Waltz successfully before it considers a transaction committed. In this sense, Waltz has the transaction authority, i.e., Waltz log is the source of truth.

Read/Write operations are done over the network. An operation may fail at any point in communication. Also the application process or waltz process may fail due to a code bug, a machine failure, etc. It is imperative to know which transactions are persisted in Waltz and which transactions are read by the application already after restart.

To address the above concerns, We designed Waltz based on a stream oriented communication instead of a RPC based communication.

![RPC communication design](/img/docs/rpc-comm.png)

Transactions are stored as a log (a series of records) and are assigned a unique transaction id (a long integer), which is monotonically increasing and dense (no gap). Waltz uses the transaction ID as a high-water mark in streaming. Waltz asks an application for its current high-water mark, the highest ID of transactions that the application consumed successfully. Based on this client high-water mark, Waltz starts streaming all transactions to the application after the high-water mark. This makes it easy for Waltz client code to discover which transactions have succeeded. Waltz clients automatically re-execute failed transactions by invoking application provided code that constructs a transaction data.

An application may consist of multiple server processes which share the same application database. In this case, each application server receives not only transactions originated from that server but all transactions available for consumption. This is necessary for application servers to do seamless failover. So, it is important to ensure that there is no duplicate processing (or processing is idempotent), the instances collectively process all transactions, and each instance applies a non-deterministic subset of transactions to the application's database. In general it should be assumed that a transaction may be processed by an application instance other than the instance which created the transaction. We provide code that coordinates processing for database applications (AbstractClientCallbacksForJDBC).

Sending all transaction data to all application server instances is wasteful. To address this issue we employ lazy loading of transaction data. The stream does not actually contain transaction data. Transaction data is loaded on demand only when an application requires it for processing.

Finally, Waltz addresses consistency issues caused by concurrent updates. The transaction system should take care of update conflicts. They can happen when concurrent transactions overwrite each other, or when a transaction is performed based on stale data. In traditional database systems, this is handled by locking. In Waltz a similar machinery is provided. Waltz implements optimistic locking. When Waltz finds a transaction conflicting with an already committed transaction, it rejects the conflicting transactions.

## An Example

TODO

## Client API

### WaltzClient

A client application creates an instance of `WaltzClient` by giving an instance `WaltzClientCallbacks` and an instance `WaltzClientConfig`. As soon as an instance of `WaltzClient` is created, it attempts to connect Zookeeper cluster (the Zookeeper connect string is specified in `WaltzClientConfig`). Waltz uses `WaltzClientCallbacks` to talk to an application.

An application call executes method to `execute` a transaction.

```
public void submit(TransactionContext context);
```

`TransactionContext` encapsulates code that builds a transaction. An application must define a subclass of `TransactionContext`.

### TransactionContext

```java

/**
 * The abstract class of the transaction context.
 */
public abstract class TransactionContext {

    public final long creationTime;

    /**
     * Class Constructor.
     */
    public TransactionContext() {
        this.creationTime = System.currentTimeMillis();
    }

    /**
     * Returns the partition id for this transaction.
     *
     * @param numPartitions the number of partitions.
     * @return the partitionId.
     */
    public abstract int partitionId(int numPartitions);

    /**
     * <p>
     * Executes the transaction. An application must implement this method.
     * </p><p>
     * The application sets the header and the data of the transaction using the builder, and optionally sets locks.
     * When this returns true, the Waltz client builds the transaction from the builder and sends an append request
     * to a Waltz server. If the client failed to send the request, it will call this method to execute the transaction
     * again.
     * </p><P>
     * If the application finds that the transaction must be ignored, this call must return false.
     * </P><P>
     * If an exception is thrown by this method, the client will call {@link TransactionContext#onException(Throwable)}.
     * </P>
     *
     * @param builder TransactionBuilder.
     * @return {@code true} if the transaction should be submitted, {@code false} if the transaction should be ignored.
     */
    public abstract boolean execute(TransactionBuilder builder);

    /**
     * A method that is called on completion of this transaction context that did not fail due to expiration or exception.
     * After this call, no retry will be attempted by the Waltz client.
     * The {@code result} parameter is {@code true} if the transaction is successfully appended to Waltz log,
     * otherwise {@code false}, i.e., the transaction is ignored.
     *
     * @param result {@code true} if the transaction is successfully appended to Waltz log, otherwise {@code false}.
     */
    public void onCompletion(boolean result) {
    }

    /**
     * A method that is called on exception.
     * After this call, no retry will be attempted by the Waltz client.
     */
    public void onException(Throwable ex) {
    }

}
```

### Waltz Client Callbacks

An application must implement `WaltzClientCallbacks` which has three methods shown below. They are invoked by `WaltzClient` to retrieve the client high-water mark, and to supply new committed transactions to the application to update application's states, and to allow the application to handle exceptions.

```java
/**
 * The interface for Waltz client callback methods.
 */
public interface WaltzClientCallbacks {

    /**
     * Returns the current high-water mark of the client application.
     * {@link WaltzClient} calls this method to know which offset to start transaction feeds from.
     *
     * @param partitionId the partition id to get client high-water mark of.
     * @return client high-water mark.
     */
    long getClientHighWaterMark(int partitionId);

    /**
     * Applies a committed transaction to the client application.
     * {@link WaltzClient} calls this method to pass a transaction information that is committed to the write ahead log.
     *
     * @param transaction a committed transaction.
     */
    void applyTransaction(Transaction transaction);

    /**
     * A method called by the Waltz client when {@link #applyTransaction(Transaction)} throws an exception.
     *
     * @param partitionId the partition id of the transaction.
     * @param transactionId the id of the transaction.
     * @param exception thrown exception by {@link #applyTransaction(Transaction)}.
     */
    void uncaughtException(int partitionId, long transactionId, Throwable exception);

}
```

Other important classes/interfaces

* `TransactionBuilder`
* `Transaction`
* `WaltzClientConfig`
* `PartitionLocalLock`
* `Serializer`

