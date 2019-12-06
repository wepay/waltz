---
id: using-mock-driver
title: Using MockDriver for Application Unit Tests
---
You can test your application code without setting up and running a Waltz cluster.
All you have to do is configure `WaltzClient` with `MockDriver`.
The mock driver provides basic functionality of Waltz servers without using network, storage or zookeeper.
It is confined in JVM that runs the driver. All transaction data are kept in JVM heap.

## A Single Client in a Test
If you need a single client in a test, create a `MockDriver` instance and set it in the client config.

```java
...
import com.wepay.waltz.client.internal.mock.MockDriver;
...
 
// Create a mock driver
MockDriver mockDriver = new MockDriver();
 
// Set the driver in the client config
WaltzClientConfig config = new WaltzClientConfig(new Properties());
config.setObject(WaltzClientConfig.MOCK_DRIVER, mockDriver);
 
// Create your Waltz client callback object, and use it to create a Waltz client
WaltzClientCallbacks callbacks = new YourCallbacks();
WaltzClient client = new WaltzClient(callbacks, config);
```

## Multiple Clients in a Test
If you need to use multiple clients in a test, you must create mock server partitions explicitly,
then, create an instance of `MockDriver` for each client using the same mock server partitions. 
Each driver must be created with a distinct client id.

```java
...
import com.wepay.waltz.client.internal.mock.MockDriver;
import com.wepay.waltz.client.internal.mock.MockServerPartition;
...
 
// Create server partitions. This will be shared by multiple drivers.
Map<Integer, MockServerPartition> serverPartitions = MockServerPartition.create(1);
 
// Create the first client
MockDriver mockDriver1 = new MockDriver(1 /*clientId*/, serverPartitions);
WaltzClientConfig config1 = new WaltzClientConfig(new Properties());
config1.setObject(WaltzClientConfig.MOCK_DRIVER, mockDriver1);
WaltzClientCallbacks callbacks1 = new YourCallbacks();
WaltzClient client1 = new WaltzClient(callbacks1, config1);
 
// Create the second client
MockDriver mockDriver2 = new MockDriver(2 /*clientId*/, serverPartitions);
WaltzClientConfig config2 = new WaltzClientConfig(new Properties());
config2.setObject(WaltzClientConfig.MOCK_DRIVER, mockDriver2);
WaltzClientCallbacks callbacks2 = new YourCallbacks();
WaltzClient client2 = new WaltzClient(callbacks2, config2);
```

## Fault Injection
In real world processing can fail for various reasons.
Waltz provides a mechanism for automatic retry of transactions in its framework.
An application developer should make sure that application logic is properly implemented according to Waltz framework.

`MockDriver` allows you to inject faults for convenience of application development.
`MockDriver` provides the following methods to inject faults.

```java
/**
 * Adds random append failures.
 * @param faultRate the fault rate
 */
public void setFaultRate(double faultRate);
 
/**
 * Forces the next append request fail.
 */
public void forceNextAppendFail();

/**
 * Forces the next lock fail.
 */
public void forceNextLockFail() {
    streamClient.forceNextLockFail();
}
```

## Other Controls
There are other controls that a test program can make use of.

```java
/**
 * Adds a random delay time in transaction processing on server side.
 * @param maxDelayMillis the max time of delay in milliseconds
 */
public void setMaxDelay(long maxDelayMillis);
 
 
/**
 * Suspends the feed.
 */
public void suspendFeed();
 
/**
 * Resumes the feed.
 */
public void resumeFeed();
```
