package com.wepay.waltz.storage.client;

import com.wepay.riff.network.MessageHandlerCallbacks;
import com.wepay.riff.network.NetworkClient;
import com.wepay.waltz.storage.common.message.SequenceMessage;
import com.wepay.waltz.storage.exception.StorageException;
import com.wepay.waltz.storage.exception.StorageRpcException;
import io.netty.handler.ssl.SslContext;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

public abstract class StorageBaseClient extends NetworkClient {
    final UUID key;
    final int numPartitions;
    final ConcurrentHashMap<Long, CompletableFuture<Object>> pendingRequests = new ConcurrentHashMap<>();
    final AtomicLong seqNum = new AtomicLong(0);
    final CompletableFuture<Object> openStorageFuture;

    public StorageBaseClient(String host, int port, SslContext sslCtx, UUID key, int numPartitions) {
        super(host, port, sslCtx);

        this.key = key;
        this.numPartitions = numPartitions;
        this.openStorageFuture = new CompletableFuture<>();
    }

    /**
     * Ensure that the storage is opened.
     */
    public void awaitOpen() throws ExecutionException {
        while (true) {
            try {
                // Ensure that the storage is opened.
                openStorageFuture.get();
                break;

            } catch (InterruptedException ex) {
                Thread.interrupted();
            } catch (ExecutionException ex) {
                close();
                throw ex;
            }
        }
    }

    protected void shutdown() {
        super.shutdown();

        StorageRpcException exception = new StorageRpcException("disconnected");
        exception.setStackTrace(new StackTraceElement[0]);

        if (!openStorageFuture.isDone()) {
            openStorageFuture.completeExceptionally(exception);
        }

        for (CompletableFuture<Object> future : pendingRequests.values()) {
            future.completeExceptionally(exception);
        }
    }

    private void openStorage() {
        if (!openStorageFuture.isDone()) {
            SequenceMessage msg = getOpenRequest();

            if (pendingRequests.putIfAbsent(msg.seqNum, openStorageFuture) != null) {
                throw new IllegalArgumentException("duplicate calls");
            }

            if (!sendMessage(msg)) {
                openStorageFuture.completeExceptionally(new StorageException("disconnected"));
                pendingRequests.remove(msg.seqNum);
            }
        }
    }

    abstract SequenceMessage getOpenRequest();

    CompletableFuture<Object> call(SequenceMessage msg) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        while (true) {
            try {
                // Ensure that the storage is opened.
                openStorageFuture.get();
                break;

            } catch (InterruptedException ex) {
                Thread.interrupted();
            } catch (ExecutionException ex) {
                close();
                future.completeExceptionally(ex);
                return future;
            }
        }

        if (pendingRequests.putIfAbsent(msg.seqNum, future) != null) {
            throw new IllegalArgumentException("duplicate calls");
        }

        if (!sendMessage(msg)) {
            future.completeExceptionally(new StorageException("disconnected"));
            pendingRequests.remove(msg.seqNum);
        }

        return future;
    }


    class MessageHandlerCallbacksImpl implements MessageHandlerCallbacks {

        @Override
        public void onChannelActive() {
            openStorage();
        }

        @Override
        public void onChannelInactive() {
            // This happens when the server was shutdown or there was a network error
            closeAsync();
        }

        @Override
        public void onWritabilityChanged(boolean isWritable) {
            // Do nothing
        }

        @Override
        public void onExceptionCaught(Throwable ex) {
            // Do nothing
        }

    }
}
