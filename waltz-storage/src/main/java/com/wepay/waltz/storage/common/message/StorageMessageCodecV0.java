package com.wepay.waltz.storage.common.message;

import com.wepay.riff.network.Message;
import com.wepay.riff.network.MessageAttributeReader;
import com.wepay.riff.network.MessageAttributeWriter;
import com.wepay.riff.network.MessageCodec;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.storage.common.SessionInfo;
import com.wepay.waltz.storage.exception.StorageRpcException;

import java.util.ArrayList;
import java.util.UUID;

public class StorageMessageCodecV0 implements MessageCodec {

    public static final StorageMessageCodecV0 INSTANCE = new StorageMessageCodecV0();

    private static final byte MAGIC_BYTE = 'S';
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
        long sessionId = reader.readLong();
        long seqNum = reader.readLong();
        int partitionId = reader.readInt();

        switch (messageType) {
            case StorageMessageType.OPEN_REQUEST:
                return new OpenRequest(new UUID(reader.readLong(), reader.readLong()), reader.readInt());

            case StorageMessageType.LAST_SESSION_INFO_REQUEST:
                return new LastSessionInfoRequest(sessionId, seqNum, partitionId);

            case StorageMessageType.LAST_SESSION_INFO_RESPONSE:
                return new LastSessionInfoResponse(sessionId, seqNum, partitionId, SessionInfo.readFrom(reader));

            case StorageMessageType.SET_LOW_WATER_MARK_REQUEST:
                return new SetLowWaterMarkRequest(sessionId, seqNum, partitionId, reader.readLong());

            case StorageMessageType.TRUNCATE_REQUEST:
                return new TruncateRequest(sessionId, seqNum, partitionId, reader.readLong());

            case StorageMessageType.APPEND_REQUEST:
                int numRecords = reader.readInt();
                ArrayList<Record> records = new ArrayList<>(numRecords);
                for (int i = 0; i < numRecords; i++) {
                    records.add(Record.readFrom(reader));
                }
                return new AppendRequest(sessionId, seqNum, partitionId, records);

            case StorageMessageType.SUCCESS_RESPONSE:
                return new SuccessResponse(sessionId, seqNum, partitionId);

            case StorageMessageType.FAILURE_RESPONSE:
                StorageRpcException exception = StorageRpcException.readFrom(reader);
                return new FailureResponse(sessionId, seqNum, partitionId, exception);

            case StorageMessageType.RECORD_HEADER_REQUEST:
                return new RecordHeaderRequest(sessionId, seqNum, partitionId, reader.readLong());

            case StorageMessageType.RECORD_HEADER_RESPONSE:
                if (reader.readBoolean()) {
                    return new RecordHeaderResponse(sessionId, seqNum, partitionId, RecordHeader.readFrom(reader));
                } else {
                    return new RecordHeaderResponse(sessionId, seqNum, partitionId, null);
                }

            case StorageMessageType.RECORD_REQUEST:
                return new RecordRequest(sessionId, seqNum, partitionId, reader.readLong());

            case StorageMessageType.RECORD_RESPONSE:
                if (reader.readBoolean()) {
                    return new RecordResponse(sessionId, seqNum, partitionId, Record.readFrom(reader));
                } else {
                    return new RecordResponse(sessionId, seqNum, partitionId, null);
                }

            case StorageMessageType.MAX_TRANSACTION_ID_REQUEST:
                return new MaxTransactionIdRequest(sessionId, seqNum, partitionId);

            case StorageMessageType.MAX_TRANSACTION_ID_RESPONSE:
                return new MaxTransactionIdResponse(sessionId, seqNum, partitionId, reader.readLong());

            case StorageMessageType.RECORD_HEADER_LIST_REQUEST:
                return new RecordHeaderListRequest(sessionId, seqNum, partitionId, reader.readLong(), reader.readInt());

            case StorageMessageType.RECORD_HEADER_LIST_RESPONSE:
                ArrayList<RecordHeader> recordHeaderList = new ArrayList<>();
                int recordHeaderListSize = reader.readInt();
                for (int i = 0; i < recordHeaderListSize; i++) {
                    recordHeaderList.add(RecordHeader.readFrom(reader));
                }
                return new RecordHeaderListResponse(sessionId, seqNum, partitionId, recordHeaderList);

            case StorageMessageType.RECORD_LIST_REQUEST:
                return new RecordListRequest(sessionId, seqNum, partitionId, reader.readLong(), reader.readInt());

            case StorageMessageType.RECORD_LIST_RESPONSE:
                ArrayList<Record> recordList = new ArrayList<>();
                int recordListSize = reader.readInt();
                for (int i = 0; i < recordListSize; i++) {
                    recordList.add(Record.readFrom(reader));
                }
                return new RecordListResponse(sessionId, seqNum, partitionId, recordList);

            default:
                throw new IllegalStateException("unknown message type: " + messageType);
        }
    }

    @Override
    public void encode(Message msg, MessageAttributeWriter writer) {
        // Encode common attributes
        writer.writeByte(msg.type());
        writer.writeLong(((StorageMessage) msg).sessionId);
        writer.writeLong(((StorageMessage) msg).seqNum);
        writer.writeInt(((StorageMessage) msg).partitionId);

        switch (msg.type()) {
            case StorageMessageType.OPEN_REQUEST:
                OpenRequest openRequest = (OpenRequest) msg;
                writer.writeLong(openRequest.key.getMostSignificantBits());
                writer.writeLong(openRequest.key.getLeastSignificantBits());
                writer.writeInt(openRequest.numPartitions);
                break;

            case StorageMessageType.LAST_SESSION_INFO_REQUEST:
                break;

            case StorageMessageType.LAST_SESSION_INFO_RESPONSE:
                LastSessionInfoResponse lastSessionInfoResponse = (LastSessionInfoResponse) msg;
                lastSessionInfoResponse.lastSessionInfo.writeTo(writer);
                break;

            case StorageMessageType.SET_LOW_WATER_MARK_REQUEST:
                SetLowWaterMarkRequest setLowWaterMarkRequest = (SetLowWaterMarkRequest) msg;
                writer.writeLong(setLowWaterMarkRequest.lowWaterMark);
                break;

            case StorageMessageType.TRUNCATE_REQUEST:
                TruncateRequest truncateRequest = (TruncateRequest) msg;
                writer.writeLong(truncateRequest.transactionId);
                break;

            case StorageMessageType.APPEND_REQUEST:
                AppendRequest appendRequest = (AppendRequest) msg;
                writer.writeInt(appendRequest.records.size());
                for (Record record : appendRequest.records) {
                    record.writeTo(writer);
                }
                break;

            case StorageMessageType.SUCCESS_RESPONSE:
                break;

            case StorageMessageType.FAILURE_RESPONSE:
                FailureResponse failureResponse = (FailureResponse) msg;
                failureResponse.exception.writeTo(writer);
                break;

            case StorageMessageType.RECORD_HEADER_REQUEST:
                RecordHeaderRequest recordHeaderRequest = (RecordHeaderRequest) msg;
                writer.writeLong(recordHeaderRequest.transactionId);
                break;

            case StorageMessageType.RECORD_HEADER_RESPONSE:
                RecordHeaderResponse recordHeaderResponse = (RecordHeaderResponse) msg;
                if (recordHeaderResponse.recordHeader != null) {
                    writer.writeBoolean(true);
                    recordHeaderResponse.recordHeader.writeTo(writer);
                } else {
                    writer.writeBoolean(false);
                }
                break;

            case StorageMessageType.RECORD_REQUEST:
                RecordRequest recordRequest = (RecordRequest) msg;
                writer.writeLong(recordRequest.transactionId);
                break;

            case StorageMessageType.RECORD_RESPONSE:
                RecordResponse recordResponse = (RecordResponse) msg;
                if (recordResponse.record != null) {
                    writer.writeBoolean(true);
                    recordResponse.record.writeTo(writer);
                } else {
                    writer.writeBoolean(false);
                }
                break;

            case StorageMessageType.MAX_TRANSACTION_ID_REQUEST:
                break;

            case StorageMessageType.MAX_TRANSACTION_ID_RESPONSE:
                MaxTransactionIdResponse maxTransactionIdResponse = (MaxTransactionIdResponse) msg;
                writer.writeLong(maxTransactionIdResponse.transactionId);
                break;

            case StorageMessageType.RECORD_HEADER_LIST_REQUEST:
                RecordHeaderListRequest recordHeaderListRequest = (RecordHeaderListRequest) msg;
                writer.writeLong(recordHeaderListRequest.transactionId);
                writer.writeInt(recordHeaderListRequest.maxNumRecords);
                break;

            case StorageMessageType.RECORD_HEADER_LIST_RESPONSE:
                RecordHeaderListResponse recordHeaderListResponse = (RecordHeaderListResponse) msg;
                ArrayList<RecordHeader> recordHeaderList = recordHeaderListResponse.recordHeaders;
                int recordHeaderListSize = recordHeaderList.size();
                writer.writeInt(recordHeaderListSize);
                for (int i = 0; i < recordHeaderListSize; i++) {
                    recordHeaderList.get(i).writeTo(writer);
                }
                break;

            case StorageMessageType.RECORD_LIST_REQUEST:
                RecordListRequest recordListRequest = (RecordListRequest) msg;
                writer.writeLong(recordListRequest.transactionId);
                writer.writeInt(recordListRequest.maxNumRecords);
                break;

            case StorageMessageType.RECORD_LIST_RESPONSE:
                RecordListResponse recordListResponse = (RecordListResponse) msg;
                ArrayList<Record> recordList = recordListResponse.records;
                int recordListSize = recordList.size();
                writer.writeInt(recordListSize);
                for (int i = 0; i < recordListSize; i++) {
                    recordList.get(i).writeTo(writer);
                }
                break;

            default:
                throw new IllegalStateException("unknown message type: " + msg.type());
        }
    }

}
