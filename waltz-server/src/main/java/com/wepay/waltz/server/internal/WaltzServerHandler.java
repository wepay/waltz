package com.wepay.waltz.server.internal;

import com.wepay.riff.network.Message;
import com.wepay.riff.network.MessageCodec;
import com.wepay.riff.network.MessageHandler;
import com.wepay.riff.network.MessageHandlerCallbacks;
import com.wepay.riff.util.Logging;
import com.wepay.waltz.common.message.AbstractMessage;
import com.wepay.waltz.common.message.CheckStorageConnectivityRequest;
import com.wepay.waltz.common.message.CheckStorageConnectivityResponse;
import com.wepay.waltz.common.message.MessageCodecV0;
import com.wepay.waltz.common.message.MessageCodecV1;
import com.wepay.waltz.common.message.MessageCodecV2;
import com.wepay.waltz.common.message.MessageType;
import com.wepay.waltz.common.message.MountRequest;
import com.wepay.waltz.common.message.ServerPartitionsAssignmentRequest;
import com.wepay.waltz.common.message.ServerPartitionsAssignmentResponse;
import com.wepay.waltz.common.metadata.ReplicaId;
import com.wepay.waltz.storage.client.StorageClient;
import com.wepay.waltz.store.Store;
import com.wepay.waltz.store.exception.StoreException;
import com.wepay.waltz.store.exception.StorePartitionClosedException;
import com.wepay.waltz.store.internal.ConnectionConfig;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;


/**
 * Implements {@link com.wepay.waltz.server.WaltzServer} message handler.
 */
public class WaltzServerHandler extends MessageHandler implements PartitionClient {

    private static final Logger LOGGER = Logging.getLogger(WaltzServerHandler.class);
    private static final int QUEUE_LOW_WATER_MARK = 300;
    private static final int QUEUE_HIGH_WATER_MARK = 600;

    private static final HashMap<Short, MessageCodec> CODECS = new HashMap<>();
    static {
        CODECS.put(MessageCodecV0.VERSION, MessageCodecV0.INSTANCE);
        CODECS.put(MessageCodecV1.VERSION, MessageCodecV1.INSTANCE);
        CODECS.put(MessageCodecV2.VERSION, MessageCodecV2.INSTANCE);
    }

    private static final String HELLO_MESSAGE = "Waltz Server";

    private final Map<Integer, Partition> partitions;
    private final Store store;
    private Integer clientId = null;
    private Long seqNum = null;

    /**
     * Class constructor.
     * @param partitions Partition IDs that are part of the {@link com.wepay.waltz.server.WaltzServer} and their corresponding {@link Partition} object.
     */
    public WaltzServerHandler(Map<Integer, Partition> partitions, Store store) {
        this(partitions, new WaltzServerHandlerCallbacks(partitions), store);
    }

    private WaltzServerHandler(Map<Integer, Partition> partitions, WaltzServerHandlerCallbacks callbacks,
                               Store store) {
        super(CODECS, HELLO_MESSAGE, callbacks, QUEUE_LOW_WATER_MARK, QUEUE_HIGH_WATER_MARK);

        this.partitions = partitions;
        this.store = store;
        callbacks.setMessageHandler(this);
    }

    @Override
    public Integer clientId() {
        return clientId;
    }

    @Override
    public Long seqNum() {
        return seqNum;
    }

    @Override
    protected void process(Message msg) throws Exception {
        if (clientId == null) {
            clientId = ((AbstractMessage) msg).reqId.clientId();
        }

        switch (msg.type()) {
            case MessageType.CHECK_STORAGE_CONNECTIVITY_REQUEST:
                Set<ReplicaId> replicaIds = store.getReplicaIds();
                Set<String> storageNodeConnectStrings =
                    replicaIds.stream().map(replicaId -> replicaId.storageNodeConnectString).collect(Collectors.toSet());
                Map<String, Boolean> storageConnectivityMap = new HashMap<String, Boolean>();
                for (String storageNodeConnectString : storageNodeConnectStrings) {
                    String[] hostAndPortArray = storageNodeConnectString.split(":");
                    String host = hostAndPortArray[0];
                    int storagePort = Integer.parseInt(hostAndPortArray[1]);
                    ConnectionConfig config = store.getConnectionConfig();
                    StorageClient storageClient = null;
                    try {
                        storageClient = new StorageClient(host, storagePort, config.sslCtx, config.key,
                            config.numPartitions, false);
                        storageClient.open();
                        storageClient.awaitOpen();
                        storageConnectivityMap.put(storageNodeConnectString, true);
                    } catch (Exception e) {
                        storageConnectivityMap.put(storageNodeConnectString, false);
                    } finally {
                        if (storageClient != null) {
                            storageClient.close();
                        }
                    }
                }
                sendMessage(new CheckStorageConnectivityResponse(((CheckStorageConnectivityRequest) msg).reqId,
                    storageConnectivityMap), true);
                break;

            case MessageType.SERVER_PARTITIONS_ASSIGNMENT_REQUEST:
                List<Integer> partitionsAssigned;

                synchronized (partitions) {
                    partitionsAssigned = new ArrayList<>(partitions.keySet());
                }
                sendMessage(new ServerPartitionsAssignmentResponse(((ServerPartitionsAssignmentRequest) msg).reqId,
                        partitionsAssigned), true);
                break;

            default:
                Partition partition = getPartition(((AbstractMessage) msg).reqId.partitionId());
                if (partition != null) {
                    try {
                        if (msg.type() == MessageType.MOUNT_REQUEST) {
                            if (seqNum == null) {
                                seqNum = ((MountRequest) msg).seqNum;
                            }
                            partition.setPartitionClient(this);
                        }

                        partition.receiveMessage(msg, this);

                    } catch (PartitionClosedException | StorePartitionClosedException ex) {
                        // Ignore
                    }
                } else {
                    Partition.partitionNotFound(msg, this);
                }
        }
    }

    private Partition getPartition(int partitionId) {
        synchronized (partitions) {
            return partitions.get(partitionId);
        }
    }

    private static class WaltzServerHandlerCallbacks implements MessageHandlerCallbacks {

        private final Map<Integer, Partition> partitions;
        private volatile WaltzServerHandler handler;

        WaltzServerHandlerCallbacks(Map<Integer, Partition> partitions) {
            this.partitions = partitions;
        }

        void setMessageHandler(WaltzServerHandler handler) {
            this.handler = handler;
        }

        @Override
        public void onChannelActive() {
        }

        @Override
        public void onChannelInactive() {
            synchronized (partitions) {
                for (Partition partition : partitions.values()) {
                    partition.removePartitionClient(handler);
                }
            }
        }

        @Override
        public void onWritabilityChanged(boolean isWritable) {
            if (isWritable) {
                synchronized (partitions) {
                    for (Partition partition : partitions.values()) {
                        try {
                            partition.resumePausedFeedContexts();

                        } catch (StoreException ex) {
                            // Ignore
                        }
                    }
                }
            }
        }

        @Override
        public void onExceptionCaught(Throwable ex) {
        }
    }
}
