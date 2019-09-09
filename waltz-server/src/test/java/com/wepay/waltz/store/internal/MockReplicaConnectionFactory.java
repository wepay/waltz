package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.storage.common.SessionInfo;
import com.wepay.waltz.storage.exception.StorageRpcException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MockReplicaConnectionFactory implements ReplicaConnectionFactory {

    private final HashMap<Integer, Partition> partitions;
    private final AtomicInteger currentSeq = new AtomicInteger(0);

    public MockReplicaConnectionFactory(int numPartitions) {
        this.partitions = new HashMap<>();
        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            partitions.put(partitionId, new Partition());
        }
    }

    public void setCurrentSession(int partitionId, long sessionId) {
        getPartition(partitionId).setCurrentSession(sessionId);
    }

    public void setLastSessionInfo(int partitionId, long lastSessionId, long lowWaterMark) {
        getPartition(partitionId).setLastSessionInfo(lastSessionId, lowWaterMark);
    }

    public SessionInfo getLastSessionInfo(int partitionId, long currentSessionId) throws StorageRpcException {
        return getPartition(partitionId).getLastSessionInfo(currentSessionId);
    }

    public void setMaxTransactionId(int partitionId, long transactionId) {
        getPartition(partitionId).setMaxTransactionId(transactionId);
    }

    public long getMaxTransactionId(int partitionId, long currentSessionId) throws StorageRpcException {
        return getPartition(partitionId).getMaxTransactionId(currentSessionId);
    }

    public long getLowWaterMark(int partitionId, long currentSessionId) throws StorageRpcException {
        return getPartition(partitionId).getLowWaterMark(currentSessionId);
    }

    public long truncate(int partitionId, long sessionId, long transactionId) throws StorageRpcException {
        return getPartition(partitionId).truncate(sessionId, transactionId);
    }

    public Record getRecord(int partitionId, long sessionId, long transactionId) throws StorageRpcException {
        return getPartition(partitionId).getRecord(sessionId, transactionId);
    }

    public void appendRecords(int partitionId, long sessionId, List<Record> records) throws StorageRpcException {
        getPartition(partitionId).appendRecords(sessionId, records);
    }

    public void setReplicaDown() {
        int currentSeq = this.currentSeq.get();

        while ((currentSeq & 1) == 0) {
            if (this.currentSeq.compareAndSet(currentSeq, currentSeq + 1)) {
                break;
            }
            currentSeq = this.currentSeq.get();
        }
    }

    public void setReplicaUp() {
        int currentSeq = this.currentSeq.get();

        while ((currentSeq & 1) == 1) {
            if (this.currentSeq.compareAndSet(currentSeq, currentSeq + 1)) {
                break;
            }
            currentSeq = this.currentSeq.get();
        }
    }

    void isUp(int seqNum) throws StorageRpcException {
        if (seqNum != currentSeq.get()) {
            throw new StorageRpcException("disconnected");
        }
    }

    @Override
    public ReplicaConnection get(int partitionId, long sessionId) throws StorageRpcException {
        int seqNum = currentSeq.get();
        if ((seqNum & 1) == 0) {
            return new MockReplicaConnection(partitionId, sessionId, seqNum, this);
        } else {
            throw new StorageRpcException("replica down");
        }
    }

    @Override
    public void close() {

    }

    public void await(int partitionId, long transactionId) throws StorageRpcException {
        int seqNum = currentSeq.get();
        if ((seqNum & 1) == 0) {
            Partition partition = getPartition(partitionId);
            synchronized (partition) {
                while (partition.getMaxTransactionId() < transactionId) {
                    try {
                        partition.wait();
                    } catch (InterruptedException ex) {
                        // Ignore
                    }
                }
            }
        }
    }

    private Partition getPartition(int partitionId) {
        Partition partition = partitions.get(partitionId);
        if (partition == null) {
            throw new IllegalArgumentException("partition not found");
        }
        return partition;
    }

    private static class Partition {

        private final HashMap<Long, Record> transactions = new HashMap<>();

        private long currentSessionId = -1L;
        private long maxTransactionId = -1L;
        private SessionInfo lastSessionInfo = new SessionInfo(-1L, -1L, -1L);

        void setCurrentSession(long sessionId) {
            synchronized (this) {
                currentSessionId = sessionId;
            }
        }

        void setLastSessionInfo(long lastSessionId, long lowWaterMark) {
            synchronized (this) {
                lastSessionInfo = new SessionInfo(lastSessionId, lowWaterMark, -1L);
            }
        }

        SessionInfo getLastSessionInfo(long sessionId) throws StorageRpcException {
            synchronized (this) {
                if (currentSessionId < sessionId) {
                    currentSessionId = sessionId;
                }
                return lastSessionInfo;
            }
        }

        void setMaxTransactionId(long transactionId) {
            synchronized (this) {
                maxTransactionId = transactionId;
            }
        }

        long getMaxTransactionId(long sessionId) throws StorageRpcException {
            synchronized (this) {
                if (currentSessionId < sessionId) {
                    currentSessionId = sessionId;
                }
                return maxTransactionId;
            }
        }

        long getMaxTransactionId() throws StorageRpcException {
            synchronized (this) {
                return maxTransactionId;
            }
        }

        long getLowWaterMark(long sessionId) throws StorageRpcException {
            synchronized (this) {
                return lastSessionInfo.lowWaterMark;
            }
        }

        long truncate(long sessionId, long transactionId) throws StorageRpcException {
            synchronized (this) {
                if (currentSessionId == sessionId) {
                    Iterator<Map.Entry<Long, Record>> iter = transactions.entrySet().iterator();
                    while (iter.hasNext()) {
                        if (iter.next().getKey() > transactionId) {
                            iter.remove();
                        }
                    }
                    if (maxTransactionId > transactionId) {
                        maxTransactionId = transactionId;
                    }
                    return maxTransactionId;
                } else {
                    throw new StorageRpcException("session expired");
                }
            }
        }

        Record getRecord(long sessionId, long transactionId) throws StorageRpcException {
            synchronized (this) {
                Record record = transactions.get(transactionId);
                if (record != null) {
                    return record;
                } else {
                    return null;
                }
            }
        }

        void appendRecords(long sessionId, List<Record> records) throws StorageRpcException {
            synchronized (this) {
                if (currentSessionId == sessionId) {
                    for (Record record : records) {
                        if (record.transactionId == maxTransactionId + 1) {
                            if (transactions.containsKey(record.transactionId)) {
                                throw new StorageRpcException("duplicate transaction");
                            }
                            transactions.put(record.transactionId, record);
                            maxTransactionId++;
                        } else {
                            throw new StorageRpcException("transaction out of order");
                        }
                    }
                } else {
                    throw new StorageRpcException("session expired");
                }
                notifyAll();
            }
        }

    }

}
