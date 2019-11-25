package com.wepay.waltz.store;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.store.exception.RecoveryAbortedException;
import com.wepay.waltz.store.exception.RecoveryFailedException;
import com.wepay.waltz.store.internal.ConnectionConfig;
import com.wepay.waltz.store.internal.RecoveryManager;
import com.wepay.waltz.store.internal.ReplicaConnection;
import com.wepay.waltz.store.internal.ReplicaSession;
import com.wepay.waltz.store.internal.StoreAppendRequest;
import com.wepay.waltz.store.internal.StoreSession;
import com.wepay.waltz.store.internal.TestReplicaSessionManager;
import com.wepay.waltz.common.metadata.PartitionMetadata;
import com.wepay.waltz.common.metadata.PartitionMetadataSerializer;
import com.wepay.waltz.common.metadata.ReplicaAssignments;
import com.wepay.waltz.common.metadata.ReplicaId;
import com.wepay.waltz.common.metadata.ReplicaState;
import com.wepay.zktools.util.State;
import com.wepay.zktools.util.StateChangeFuture;
import com.wepay.zktools.util.Uninterruptibly;
import com.wepay.zktools.zookeeper.NodeData;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import io.netty.handler.ssl.SslContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class TestUtils {

    private static final Random rand = new Random();

    private TestUtils() {
    }

    public static ConnectionConfig makeConnectionConfig(int numPartitions, UUID key) throws GeneralSecurityException, IOException {
        return new ConnectionConfig(null, key, numPartitions, 100, 100);
    }

    public static ConnectionConfig makeConnectionConfig(int numPartitions, UUID key, SslContext sslContext) throws GeneralSecurityException, IOException {
        return new ConnectionConfig(sslContext, key, numPartitions, 100, 100);
    }

    public static ReplicaAssignments makeReplicaAssignment(int numPartitions, List<String> connectStrings) {
        HashMap<String, int[]> assignments  = new HashMap<>();

        for (String connectString : connectStrings) {
            int[] partitionIds = new int[numPartitions];
            for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                partitionIds[partitionId] = partitionId;
            }
            assignments.put(connectString, partitionIds);
        }
        return new ReplicaAssignments(assignments);
    }

    public static void setUpMetadata(
        int generation,
        long currentSessionId,
        List<Long> lastSessionIds,
        List<Long> lowWaterMarks,
        ZooKeeperClient zkClient,
        ZNode znode
    ) throws Exception {
        zkClient.createPath(znode);

        HashMap<ReplicaId, ReplicaState> replicaStates = new HashMap<>();
        for (int i = 0; i < lastSessionIds.size(); i++) {
            ReplicaId replicaId = new ReplicaId(0, String.format(TestReplicaSessionManager.CONNECT_STRING_TEMPLATE, i));
            replicaStates.put(replicaId, new ReplicaState(replicaId, lastSessionIds.get(i), lowWaterMarks.get(i)));
        }
        PartitionMetadata partitionMetadata =
            new PartitionMetadata(generation, currentSessionId, replicaStates);

        zkClient.setData(znode, partitionMetadata, PartitionMetadataSerializer.INSTANCE);
    }

    public static PartitionMetadata getMetadata(ZooKeeperClient zkClient, ZNode znode) throws Exception {
        NodeData<PartitionMetadata> nodeData = zkClient.getData(znode, PartitionMetadataSerializer.INSTANCE);

        return nodeData.value;
    }

    public static boolean awaitMetadata(int numReplicas, long currentSessionId, ZooKeeperClient zkClient, ZNode znode) throws Exception {
        CountDownLatch metadataDone = new CountDownLatch(1);
        zkClient.watch(
            znode, n -> {
                PartitionMetadata metadata = n.value;
                if (metadata != null) {
                    int count = 0;
                    for (ReplicaState replicaState : metadata.replicaStates.values()) {
                        if (currentSessionId == replicaState.sessionId) {
                            count++;
                        }
                    }
                    if (count == numReplicas) {
                        metadataDone.countDown();
                    }
                }
            },
            PartitionMetadataSerializer.INSTANCE,
            true
        );
        return metadataDone.await(10000, TimeUnit.MILLISECONDS);
    }

    public static ArrayList<StoreAppendRequest> makeStoreAppendRequests(long startTransactionId, long endTransactionId) {
        ArrayList<StoreAppendRequest> requests = new ArrayList<>();

        for (long transactionId = startTransactionId; transactionId < endTransactionId; transactionId++) {
            byte[] data = data(transactionId);
            StoreAppendRequest request = new StoreAppendRequest(reqId(), rand.nextInt(), data, Utils.checksum(data), null);
            requests.add(request);
        }

        return requests;
    }

    public static ReqId reqId() {
        return new ReqId(rand.nextLong(), rand.nextLong());
    }

    public static Record record(long transactionId) {
        return record(reqId(), transactionId);
    }

    public static Record record(ReqId reqId, long transactionId) {
        byte[] data = data(transactionId);
        int checksum = Utils.checksum(data);
        return new Record(transactionId, reqId, rand.nextInt(), data, checksum);
    }

    public static ArrayList<Record> records(long from, long until) {
        ArrayList<Record> records = new ArrayList<>();
        for (long txid = from; txid < until; txid++) {
            records.add(record(txid));
        }
        return records;
    }

    public static byte[] data(long transactionId) {
        return Long.toOctalString(transactionId).getBytes(StandardCharsets.UTF_8);
    }

    public static long syncAppend(StoreSession session, Record record) throws Exception {
        State<Long> appendState = new State<>(null);
        StateChangeFuture<Long> f = appendState.watch();
        session.append(new StoreAppendRequest(record.reqId, record.header, record.data, record.checksum, appendState::set));
        while (!f.isDone()) {
            session.flush();
        }
        return f.get();
    }

    public static MockRecoveryManager mockRecoveryManager(int quorum, long highWaterMark) {
        return new MockRecoveryManager(quorum, highWaterMark);
    }

    public static class MockRecoveryManager implements RecoveryManager {

        private final long highWaterMark;
        private final int quorum;
        private final HashSet<ReplicaId> recoveredReplicas = new HashSet<>();
        private RecoveryAbortedException recoveryAborted = null;

        public MockRecoveryManager(int quorum, long highWaterMark) {
            this.highWaterMark = highWaterMark;
            this.quorum = quorum;
        }

        @Override
        public void start(ArrayList<ReplicaSession> replicaSessions) {
        }

        @Override
        public void abort(Throwable cause) {
            fail(cause);
        }

        private RecoveryFailedException fail(Throwable cause) {
            Runnable handler = null;
            synchronized (this) {
                if (recoveryAborted == null) {
                    recoveryAborted = new RecoveryAbortedException(cause);
                }
                notifyAll();
            }

            if (cause instanceof RecoveryFailedException) {
                return (RecoveryFailedException) cause;
            } else {
                return new RecoveryFailedException("failed to recover", cause);
            }
        }

        @Override
        public long start(ReplicaId replicaId, ReplicaConnection connection) throws RecoveryFailedException {
            synchronized (this) {
                try {
                    connection.lastSessionInfo();

                    if (recoveryAborted != null) {
                        throw recoveryAborted;
                    }
                    connection.setLowWaterMark(highWaterMark);
                    notifyAll();
                    return highWaterMark;

                } catch (RecoveryAbortedException ex) {
                    throw ex;
                } catch (Throwable ex) {
                    throw fail(ex);
                }
            }
        }

        @Override
        public void end(ReplicaId replicaId) {
            synchronized (this) {
                recoveredReplicas.add(replicaId);

                while (recoveredReplicas.size() < quorum && recoveryAborted == null) {
                    Uninterruptibly.run(this::wait);
                }

                notifyAll();
            }
        }

        @Override
        public long highWaterMark() throws RecoveryFailedException {
            synchronized (this) {
                while (recoveredReplicas.size() < quorum && recoveryAborted == null) {
                    Uninterruptibly.run(this::wait);
                }

                if (recoveryAborted != null) {
                    throw recoveryAborted;
                }
                return highWaterMark;
            }
        }

    }

}
