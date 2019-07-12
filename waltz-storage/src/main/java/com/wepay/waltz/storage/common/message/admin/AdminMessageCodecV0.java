package com.wepay.waltz.storage.common.message.admin;

import com.wepay.riff.network.Message;
import com.wepay.riff.network.MessageAttributeReader;
import com.wepay.riff.network.MessageAttributeWriter;
import com.wepay.riff.network.MessageCodec;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.storage.common.SessionInfo;
import com.wepay.waltz.storage.exception.StorageRpcException;

import java.util.ArrayList;
import java.util.UUID;

public class AdminMessageCodecV0 implements MessageCodec {
    public static final AdminMessageCodecV0 INSTANCE = new AdminMessageCodecV0();

    private static final byte MAGIC_BYTE = 'A';
    private static final short VERSION = 0;

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
        long seqNum = reader.readLong();

        switch (messageType) {
            case AdminMessageType.SUCCESS_RESPONSE:
                return new AdminSuccessResponse(seqNum);

            case AdminMessageType.FAILURE_RESPONSE:
                StorageRpcException exception = StorageRpcException.readFrom(reader);
                return new AdminFailureResponse(seqNum, exception);

            case AdminMessageType.OPEN_REQUEST:
                return new AdminOpenRequest(new UUID(reader.readLong(), reader.readLong()), reader.readInt());

            case AdminMessageType.PARTITION_AVAILABLE_REQUEST:
                return new PartitionAvailableRequest(seqNum, reader.readInt(), reader.readBoolean());

            case AdminMessageType.PARTITION_ASSIGNMENT_REQUEST:
                return new PartitionAssignmentRequest(seqNum, reader.readInt(), reader.readBoolean(), reader.readBoolean());

            case AdminMessageType.RECORD_LIST_REQUEST:
                return new RecordListRequest(seqNum, reader.readInt(), reader.readLong(), reader.readInt());

            case AdminMessageType.RECORD_LIST_RESPONSE:
                ArrayList<Record> recordList = new ArrayList<>();
                int partitionId = reader.readInt();
                int recordListSize = reader.readInt();
                for (int i = 0; i < recordListSize; i++) {
                    recordList.add(Record.readFrom(reader));
                }
                return new RecordListResponse(seqNum, partitionId, recordList);

            case AdminMessageType.LAST_SESSION_INFO_REQUEST:
                return new LastSessionInfoRequest(seqNum, reader.readInt());

            case AdminMessageType.LAST_SESSION_INFO_RESPONSE:
                return new LastSessionInfoResponse(seqNum, reader.readInt(), SessionInfo.readFrom(reader));

            case AdminMessageType.METRICS_REQUEST:
                return new MetricsRequest(seqNum);

            case AdminMessageType.METRICS_RESPONSE:
                String metricsJson = reader.readString();
                return new MetricsResponse(seqNum, metricsJson);

            default:
                throw new IllegalStateException("unknown message type: " + messageType);
        }
    }

    @Override
    public void encode(Message msg, MessageAttributeWriter writer) {
        // Encode common attributes
        writer.writeByte(msg.type());
        writer.writeLong(((AdminMessage) msg).seqNum);

        switch (msg.type()) {
            case AdminMessageType.OPEN_REQUEST:
                AdminOpenRequest openRequest = (AdminOpenRequest) msg;
                writer.writeLong(openRequest.key.getMostSignificantBits());
                writer.writeLong(openRequest.key.getLeastSignificantBits());
                writer.writeInt(openRequest.numPartitions);
                break;

            case AdminMessageType.SUCCESS_RESPONSE:
                break;

            case AdminMessageType.FAILURE_RESPONSE:
                AdminFailureResponse failureResponse = (AdminFailureResponse) msg;
                failureResponse.exception.writeTo(writer);
                break;

            case AdminMessageType.PARTITION_AVAILABLE_REQUEST:
                PartitionAvailableRequest partitionAvailableRequest = (PartitionAvailableRequest) msg;
                writer.writeInt(partitionAvailableRequest.partitionId);
                writer.writeBoolean(partitionAvailableRequest.toggled);
                break;

            case AdminMessageType.PARTITION_ASSIGNMENT_REQUEST:
                PartitionAssignmentRequest partitionAssignmentRequest = (PartitionAssignmentRequest) msg;
                writer.writeInt(partitionAssignmentRequest.partitionId);
                writer.writeBoolean(partitionAssignmentRequest.toggled);
                writer.writeBoolean(partitionAssignmentRequest.deleteStorageFiles);
                break;

            case AdminMessageType.METRICS_REQUEST:
                break;

            case AdminMessageType.METRICS_RESPONSE:
                MetricsResponse metricsResponse = (MetricsResponse) msg;
                String metricsJson = metricsResponse.metricsJson;
                writer.writeString(metricsJson);
                break;

            case AdminMessageType.RECORD_LIST_REQUEST:
                RecordListRequest recordListRequest = (RecordListRequest) msg;
                writer.writeInt(recordListRequest.partitionId);
                writer.writeLong(recordListRequest.transactionId);
                writer.writeInt(recordListRequest.maxNumRecords);
                break;

            case AdminMessageType.RECORD_LIST_RESPONSE:
                RecordListResponse recordListResponse = (RecordListResponse) msg;
                writer.writeInt(recordListResponse.partitionId);
                ArrayList<Record> recordList = recordListResponse.records;
                int recordListSize = recordList.size();
                writer.writeInt(recordListSize);
                for (Record record : recordList) {
                    record.writeTo(writer);
                }
                break;

            case AdminMessageType.LAST_SESSION_INFO_REQUEST:
                LastSessionInfoRequest lastSessionInfoRequest = (LastSessionInfoRequest) msg;
                writer.writeInt(lastSessionInfoRequest.partitionId);
                break;

            case AdminMessageType.LAST_SESSION_INFO_RESPONSE:
                LastSessionInfoResponse lastSessionInfoResponse = (LastSessionInfoResponse) msg;
                writer.writeInt(lastSessionInfoResponse.partitionId);
                lastSessionInfoResponse.lastSessionInfo.writeTo(writer);
                break;

            default:
                throw new IllegalStateException("unknown message type: " + msg.type());
        }
    }

}
