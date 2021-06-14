package com.wepay.waltz.common.message;

import com.wepay.riff.network.Message;
import com.wepay.riff.network.MessageAttributeReader;
import com.wepay.riff.network.MessageAttributeWriter;
import com.wepay.riff.network.MessageCodec;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.exception.RpcException;

public class MessageCodecV0 implements MessageCodec {

    public static final short VERSION = 0;
    public static final MessageCodecV0 INSTANCE = new MessageCodecV0();

    private static final byte MAGIC_BYTE = 'L';
    private static final int[] NO_LOCKS = new int[0];

    @Override
    public byte magicByte() {
        return MAGIC_BYTE;
    }

    @Override
    public short version() {
        return VERSION;
    }

    @Override
    public Message decode(MessageAttributeReader reader) {
        // Decode common attributes
        byte messageType = reader.readByte();
        ReqId reqId = ReqId.readFrom(reader);
        long transactionId;
        int header;
        byte[] data;
        int checksum;

        switch (messageType) {
            case MessageType.MOUNT_REQUEST:
                long clientHighWaterMark = reader.readLong();
                long seqNum = reader.readLong();
                return new MountRequest(reqId, clientHighWaterMark, seqNum);

            case MessageType.MOUNT_RESPONSE:
                boolean partitionReady = reader.readBoolean();
                boolean partitionException = reader.readBoolean();
                return new MountResponse(reqId, partitionReady, partitionException);

            case MessageType.APPEND_REQUEST:
                transactionId = reader.readLong(); // client High-water mark
                int[] writeLockRequest = reader.readIntArray();
                header = reader.readInt();
                data = reader.readByteArray();
                checksum = reader.readInt();
                Utils.verifyChecksum(messageType, data, checksum);
                return new AppendRequest(reqId, transactionId, writeLockRequest, NO_LOCKS, NO_LOCKS, header, data, checksum);

            case MessageType.FEED_REQUEST:
                transactionId = reader.readLong(); // client High-water mark
                return new FeedRequest(reqId, transactionId);

            case MessageType.FEED_DATA:
                transactionId = reader.readLong();
                header = reader.readInt();
                return new FeedData(reqId, transactionId, header);

            case MessageType.FEED_SUSPENDED:
                return new FeedSuspended(reqId);

            case MessageType.TRANSACTION_DATA_REQUEST:
                transactionId = reader.readLong();
                return new TransactionDataRequest(reqId, transactionId);

            case MessageType.TRANSACTION_DATA_RESPONSE:
                transactionId = reader.readLong();
                if (reader.readBoolean()) {
                    data = reader.readByteArray();
                    checksum = reader.readInt();
                    Utils.verifyChecksum(messageType, data, checksum);
                    return new TransactionDataResponse(reqId, transactionId, data, checksum);
                } else {
                    RpcException exception = new RpcException(reader.readString());
                    return new TransactionDataResponse(reqId, transactionId, exception);
                }

            case MessageType.FLUSH_REQUEST:
                return new FlushRequest(reqId);

            case MessageType.FLUSH_RESPONSE:
                transactionId = reader.readLong();
                return new FlushResponse(reqId, transactionId);

            case MessageType.HIGH_WATER_MARK_REQUEST:
                return new HighWaterMarkRequest(reqId);

            case MessageType.HIGH_WATER_MARK_RESPONSE:
                transactionId = reader.readLong();
                return new HighWaterMarkResponse(reqId, transactionId);

            case MessageType.LOCK_FAILURE:
                transactionId = reader.readLong();
                return new LockFailure(reqId, transactionId);

            default:
                throw new IllegalStateException("unknown message type: " + messageType);
        }
    }

    @Override
    public void encode(Message msg, MessageAttributeWriter writer) {
        // Encode common attributes
        writer.writeByte(msg.type());
        ((AbstractMessage) msg).reqId.writeTo(writer);

        switch (msg.type()) {
            case MessageType.MOUNT_REQUEST:
                MountRequest mountRequest = (MountRequest) msg;
                writer.writeLong(mountRequest.clientHighWaterMark);
                writer.writeLong(mountRequest.seqNum);
                break;

            case MessageType.MOUNT_RESPONSE:
                MountResponse mountResponse = (MountResponse) msg;
                writer.writeBoolean(mountResponse.partitionReady);
                break;

            case MessageType.APPEND_REQUEST:
                AppendRequest appendRequest = (AppendRequest) msg;

                if (appendRequest.readLockRequest.length > 0) {
                    throw new UnsupportedOperationException(
                        "read locks not supported, upgrade servers"
                    );
                }
                if (appendRequest.appendLockRequest.length > 0) {
                    throw new UnsupportedOperationException(
                        "append locks not supported, upgrade servers"
                    );
                }

                writer.writeLong(appendRequest.clientHighWaterMark);
                writer.writeIntArray(appendRequest.writeLockRequest);
                writer.writeInt(appendRequest.header);
                writer.writeByteArray(appendRequest.data);
                writer.writeInt(appendRequest.checksum);
                break;

            case MessageType.FEED_REQUEST:
                FeedRequest feedRequest = (FeedRequest) msg;
                writer.writeLong(feedRequest.clientHighWaterMark);
                break;

            case MessageType.FEED_DATA:
                FeedData feedData = (FeedData) msg;
                writer.writeLong(feedData.transactionId);
                writer.writeInt(feedData.header);
                break;

            case MessageType.FEED_SUSPENDED:
                break;

            case MessageType.TRANSACTION_DATA_REQUEST:
                TransactionDataRequest dataRequest = (TransactionDataRequest) msg;
                writer.writeLong(dataRequest.transactionId);
                break;

            case MessageType.TRANSACTION_DATA_RESPONSE:
                TransactionDataResponse dataResponse = (TransactionDataResponse) msg;
                writer.writeLong(dataResponse.transactionId);
                if (dataResponse.data != null) {
                    writer.writeBoolean(true);
                    writer.writeByteArray(dataResponse.data);
                    writer.writeInt(dataResponse.checksum);
                } else if (dataResponse.exception != null) {
                    writer.writeBoolean(false);
                    writer.writeString(dataResponse.exception.getMessage());
                } else {
                    throw new IllegalStateException("corrupted message: " + msg.type());
                }
                break;

            case MessageType.FLUSH_REQUEST:
                break;

            case MessageType.FLUSH_RESPONSE:
                FlushResponse flushResponse = (FlushResponse) msg;
                writer.writeLong(flushResponse.transactionId);
                break;

            case MessageType.HIGH_WATER_MARK_REQUEST:
                break;

            case MessageType.HIGH_WATER_MARK_RESPONSE:
                HighWaterMarkResponse highWaterMarkResponse = (HighWaterMarkResponse) msg;
                writer.writeLong(highWaterMarkResponse.transactionId);
                break;

            case MessageType.LOCK_FAILURE:
                LockFailure lockFailure = (LockFailure) msg;
                writer.writeLong(lockFailure.transactionId);
                break;

            default:
                throw new IllegalStateException("unknown message type: " + msg.type());
        }
    }

}
