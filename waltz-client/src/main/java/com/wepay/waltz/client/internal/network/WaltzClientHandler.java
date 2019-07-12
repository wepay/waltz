package com.wepay.waltz.client.internal.network;

import com.wepay.riff.network.Message;
import com.wepay.riff.network.MessageCodec;
import com.wepay.riff.network.MessageHandler;
import com.wepay.riff.network.MessageProcessingThreadPool;
import com.wepay.riff.util.Logging;
import com.wepay.waltz.common.message.AbstractMessage;
import com.wepay.waltz.common.message.FeedData;
import com.wepay.waltz.common.message.FlushResponse;
import com.wepay.waltz.common.message.LockFailure;
import com.wepay.waltz.common.message.MessageCodecV0;
import com.wepay.waltz.common.message.MessageCodecV1;
import com.wepay.waltz.common.message.MessageType;
import com.wepay.waltz.common.message.MountRequest;
import com.wepay.waltz.common.message.MountResponse;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.common.message.TransactionDataResponse;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class WaltzClientHandler extends MessageHandler {

    private static final Logger logger = Logging.getLogger(WaltzClientHandler.class);

    private static final HashMap<Short, MessageCodec> CODECS = new HashMap<>();
    static {
        CODECS.put(MessageCodecV0.VERSION, MessageCodecV0.INSTANCE);
        CODECS.put(MessageCodecV1.VERSION, MessageCodecV1.INSTANCE);
    }

    private static final String HELLO_MESSAGE = "Waltz Client";

    private final WaltzClientHandlerCallbacks handlerCallbacks;

    private ConcurrentHashMap<Integer, ReqId> feedSessions = new ConcurrentHashMap<>();

    public WaltzClientHandler(WaltzClientHandlerCallbacks handlerCallbacks, MessageProcessingThreadPool messageProcessingThreadPool) {
        super(CODECS, HELLO_MESSAGE, handlerCallbacks, 30, 60, messageProcessingThreadPool);
        this.handlerCallbacks = handlerCallbacks;
    }

    @Override
    protected Integer extractProcessorId(Message msg) {
        return ((AbstractMessage) msg).reqId.partitionId();
    }

    @Override
    protected void process(Message msg) {
        ReqId reqId = ((AbstractMessage) msg).reqId;
        int partitionId = reqId.partitionId();
        switch (msg.type()) {
            case MessageType.MOUNT_RESPONSE:
                if (reqId.eq(feedSessions.get(partitionId))) {
                    MountResponse r = (MountResponse) msg;
                    if (r.partitionReady) {
                        handlerCallbacks.onPartitionMounted(partitionId, reqId);
                    } else {
                        // We may retry if the partition is still considered to be assigned to this server
                        handlerCallbacks.onPartitionNotReady(partitionId);
                    }
                } else {
                    logger.info("obsolete session");
                }
                break;

            case MessageType.FEED_DATA:
                FeedData feedData = (FeedData) msg;
                handlerCallbacks.onTransactionIdReceived(feedData.transactionId, feedData.header, feedData.reqId);
                break;

            case MessageType.FEED_SUSPENDED:
                if (reqId.eq(feedSessions.get(partitionId))) {
                    handlerCallbacks.onFeedSuspended(partitionId, reqId);
                }
                break;

            case MessageType.TRANSACTION_DATA_RESPONSE:
                TransactionDataResponse transactionDataResponse = (TransactionDataResponse) msg;
                handlerCallbacks.onTransactionDataReceived(
                    partitionId,
                    transactionDataResponse.transactionId,
                    transactionDataResponse.data,
                    transactionDataResponse.checksum,
                    transactionDataResponse.exception
                );
                break;

            case MessageType.FLUSH_RESPONSE:
                FlushResponse flushResponse = (FlushResponse) msg;
                handlerCallbacks.onFlushCompleted(flushResponse.reqId, flushResponse.transactionId);
                break;

            case MessageType.LOCK_FAILURE:
                handlerCallbacks.onLockFailed((LockFailure) msg);
                break;

            default:
                throw new IllegalArgumentException("message not handled: messageType=" + msg.type());
        }
    }

    @Override
    public boolean sendMessage(Message msg, boolean flush) {
        if (msg.type() == MessageType.MOUNT_REQUEST) {
            ReqId reqId = ((MountRequest) msg).reqId;
            feedSessions.put(reqId.partitionId(), reqId);
        }

        return super.sendMessage(msg, flush);
    }

}
