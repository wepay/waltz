package com.wepay.waltz.common.message;

import com.wepay.riff.network.Message;
import com.wepay.riff.network.MessageAttributeReader;
import com.wepay.riff.network.MessageAttributeWriter;
import com.wepay.riff.network.MessageCodec;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.exception.RpcException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageCodecV2 implements MessageCodec {

    public static final short VERSION = 2;
    public static final MessageCodecV2 INSTANCE = new MessageCodecV2();

    private static final byte MAGIC_BYTE = 'L';

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
        boolean result;

        switch (messageType) {
            case MessageType.MOUNT_REQUEST:
                long clientHighWaterMark = reader.readLong();
                long seqNum = reader.readLong();
                return new MountRequest(reqId, clientHighWaterMark, seqNum);

            case MessageType.MOUNT_RESPONSE:
                boolean partitionReady = reader.readBoolean();
                return new MountResponse(reqId, partitionReady ? MountResponse.PartitionState.READY : MountResponse.PartitionState.NOT_READY);

            case MessageType.APPEND_REQUEST:
                transactionId = reader.readLong(); // client High-water mark
                int[] writeLockRequest = reader.readIntArray();
                int[] readLockRequest = reader.readIntArray();
                int[] appendLockRequest = reader.readIntArray();
                header = reader.readInt();
                data = reader.readByteArray();
                checksum = reader.readInt();
                Utils.verifyChecksum(messageType, data, checksum);
                return new AppendRequest(
                    reqId,
                    transactionId,
                    writeLockRequest,
                    readLockRequest,
                    appendLockRequest,
                    header,
                    data,
                    checksum
                );

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

            case MessageType.CHECK_STORAGE_CONNECTIVITY_REQUEST:
                return new CheckStorageConnectivityRequest(reqId);

            case MessageType.CHECK_STORAGE_CONNECTIVITY_RESPONSE:
                int size = reader.readInt();
                Map<String, Boolean> storageConnectivityMap = new HashMap<>();
                for (int i = 0; i < size; i++) {
                    storageConnectivityMap.put(reader.readString(), reader.readBoolean());
                }
                return new CheckStorageConnectivityResponse(reqId, storageConnectivityMap);

            case MessageType.SERVER_PARTITIONS_ASSIGNMENT_REQUEST:
                return new ServerPartitionsAssignmentRequest(reqId);

            case MessageType.SERVER_PARTITIONS_ASSIGNMENT_RESPONSE:
                return new ServerPartitionsAssignmentResponse(reqId, buildListReader(reader));

            case MessageType.ADD_PREFERRED_PARTITION_REQUEST:
                return new AddPreferredPartitionRequest(reqId, buildListReader(reader));

            case MessageType.ADD_PREFERRED_PARTITION_RESPONSE:
                result = reader.readBoolean();
                return new AddPreferredPartitionResponse(reqId, result);

            case MessageType.REMOVE_PREFERRED_PARTITION_REQUEST:
                return new RemovePreferredPartitionRequest(reqId, buildListReader(reader));

            case MessageType.REMOVE_PREFERRED_PARTITION_RESPONSE:
                result = reader.readBoolean();
                return new RemovePreferredPartitionResponse(reqId, result);

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
                writer.writeBoolean(mountResponse.partitionState == MountResponse.PartitionState.READY);
                break;

            case MessageType.APPEND_REQUEST:
                AppendRequest appendRequest = (AppendRequest) msg;
                writer.writeLong(appendRequest.clientHighWaterMark);
                writer.writeIntArray(appendRequest.writeLockRequest);
                writer.writeIntArray(appendRequest.readLockRequest);
                writer.writeIntArray(appendRequest.appendLockRequest);
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

            case MessageType.CHECK_STORAGE_CONNECTIVITY_REQUEST:
                break;

            case MessageType.CHECK_STORAGE_CONNECTIVITY_RESPONSE:
                CheckStorageConnectivityResponse checkStorageConnectivityResponse =
                    (CheckStorageConnectivityResponse) msg;
                int size = checkStorageConnectivityResponse.storageConnectivityMap.size();
                writer.writeInt(size);
                Map<String, Boolean> storageConnectivityMap = checkStorageConnectivityResponse.storageConnectivityMap;
                for (Map.Entry<String, Boolean> storageConnectionEntry : storageConnectivityMap.entrySet()) {
                    writer.writeString(storageConnectionEntry.getKey());
                    writer.writeBoolean(storageConnectionEntry.getValue());
                }
                break;

            case MessageType.SERVER_PARTITIONS_ASSIGNMENT_REQUEST:
                break;

            case MessageType.SERVER_PARTITIONS_ASSIGNMENT_RESPONSE:
                ServerPartitionsAssignmentResponse serverPartitionsAssignmentResponse =
                        (ServerPartitionsAssignmentResponse) msg;
                List<Integer> partitionsAssigned = serverPartitionsAssignmentResponse.serverPartitionAssignments;
                writeListWriter(writer, partitionsAssigned);
                break;

            case MessageType.ADD_PREFERRED_PARTITION_REQUEST:
                AddPreferredPartitionRequest addPreferredPartitionRequest = (AddPreferredPartitionRequest) msg;
                writeListWriter(writer, addPreferredPartitionRequest.partitionIds);
                break;

            case MessageType.ADD_PREFERRED_PARTITION_RESPONSE:
                AddPreferredPartitionResponse addPreferredPartitionResponse = (AddPreferredPartitionResponse) msg;
                writer.writeBoolean(addPreferredPartitionResponse.result);
                break;

            case MessageType.REMOVE_PREFERRED_PARTITION_REQUEST:
                RemovePreferredPartitionRequest removePreferredPartitionRequest = (RemovePreferredPartitionRequest) msg;
                writeListWriter(writer, removePreferredPartitionRequest.partitionIds);
                break;

            case MessageType.REMOVE_PREFERRED_PARTITION_RESPONSE:
                RemovePreferredPartitionResponse removePreferredPartitionResponse =
                    (RemovePreferredPartitionResponse) msg;
                writer.writeBoolean(removePreferredPartitionResponse.result);
                break;

            default:
                throw new IllegalStateException("unknown message type: " + msg.type());
        }
    }

    private List<Integer> buildListReader(MessageAttributeReader reader) {
        int listSize = reader.readInt();
        List<Integer> list = new ArrayList<>(listSize);
        for (int i = 0; i < listSize; i++) {
            list.add(reader.readInt());
        }
        return list;
    }

    private void writeListWriter(MessageAttributeWriter writer, List<Integer> list) {
        writer.writeInt(list.size());
        for (Integer partition : list) {
            writer.writeInt(partition);
        }
    }
}
