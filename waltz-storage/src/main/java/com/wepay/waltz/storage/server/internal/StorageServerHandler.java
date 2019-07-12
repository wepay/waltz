package com.wepay.waltz.storage.server.internal;

import com.wepay.riff.network.Message;
import com.wepay.riff.network.MessageCodec;
import com.wepay.riff.network.MessageHandler;
import com.wepay.riff.util.Logging;
import com.wepay.waltz.storage.common.message.FailureResponse;
import com.wepay.waltz.storage.common.message.OpenRequest;
import com.wepay.waltz.storage.common.message.StorageMessage;
import com.wepay.waltz.storage.common.message.StorageMessageCodecV0;
import com.wepay.waltz.storage.common.message.StorageMessageType;
import com.wepay.waltz.storage.common.message.SuccessResponse;
import com.wepay.waltz.storage.exception.StorageException;
import com.wepay.waltz.storage.exception.StorageRpcException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;

import java.util.HashMap;

public class StorageServerHandler extends MessageHandler implements PartitionClient {

    private static final Logger logger = Logging.getLogger(StorageServerHandler.class);

    private static final int QUEUE_LOW_WATER_MARK = 300;
    private static final int QUEUE_HIGH_WATER_MARK = 600;

    private static final HashMap<Short, MessageCodec> CODECS = new HashMap<>();
    static {
        CODECS.put((short) 0, StorageMessageCodecV0.INSTANCE);
    }

    private static final String HELLO_MESSAGE = "Waltz Storage Server";

    private final StorageManager storageManager;

    public StorageServerHandler(StorageManager storageManager) {
        super(CODECS, HELLO_MESSAGE, null, QUEUE_LOW_WATER_MARK, QUEUE_HIGH_WATER_MARK);

        this.storageManager = storageManager;
    }

    @Override
    protected void process(Message msg) throws Exception {
        StorageMessage message = (StorageMessage) msg;

        switch (message.type()) {
            case StorageMessageType.OPEN_REQUEST:
                try {
                    storageManager.open(((OpenRequest) message).key, ((OpenRequest) message).numPartitions);
                    success();

                } catch (IllegalArgumentException ex) {
                    failure(new StorageException("wrong key format"));
                } catch (StorageException ex) {
                    failure(ex);
                }
                break;

            default:
                Partition partition = storageManager.getPartition(((StorageMessage) msg).partitionId);

                if (partition != null) {
                    partition.receiveMessage(msg, this);
                } else {
                    failure(new Exception("Partition:" + message.partitionId + " is not assigned."), message);
                }
        }
    }

    private void success() {
        sendMessage(new SuccessResponse(-1L, -1L, -1), true);
    }

    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD")
    private void success(StorageMessage msg) {
        sendMessage(new SuccessResponse(msg.sessionId, msg.seqNum, msg.partitionId), true);
    }

    private void failure(Exception ex) {
        logger.error("unable to open storage", ex);
        sendMessage(new FailureResponse(-1L, -1L, -1, new StorageRpcException(ex)), true);
    }

    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD")
    private void failure(Exception ex, StorageMessage msg) {
        sendMessage(new FailureResponse(msg.sessionId, msg.seqNum, msg.partitionId, new StorageRpcException(ex)), true);
    }
}
