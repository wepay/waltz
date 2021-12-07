package com.wepay.waltz.storage.common.message.admin;

import com.wepay.riff.message.ByteArrayMessageAttributeReader;
import com.wepay.riff.message.ByteArrayMessageAttributeWriter;
import com.wepay.riff.network.Message;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.storage.common.SessionInfo;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AdminMessageCodecV0Test {
    private final AdminMessageCodecV0 codec = new AdminMessageCodecV0();
    private final Random rand = new Random();

    @Test
    public void testCodecVersion() {
        assertEquals(0, codec.version());
    }

    @Test
    public void testSetPartitionAvailableRequest() {
        PartitionAvailableRequest partitionAvailableRequest1 = new PartitionAvailableRequest(2, Arrays.asList(3, 4), true);
        PartitionAvailableRequest partitionAvailableRequest2 = encodeThenDecode(partitionAvailableRequest1);
        assertEquals(AdminMessageType.PARTITION_AVAILABLE_REQUEST, partitionAvailableRequest1.type());
        assertEquals(partitionAvailableRequest1.type(), partitionAvailableRequest2.type());
        assertTrue(partitionAvailableRequest1.partitionsIds.containsAll(partitionAvailableRequest2.partitionsIds));
        assertEquals(partitionAvailableRequest1.seqNum, partitionAvailableRequest2.seqNum);
        assertEquals(partitionAvailableRequest1.toggled, partitionAvailableRequest2.toggled);
    }

    @Test
    public void testPartitionAssignmentRequest() {
        PartitionAssignmentRequest partitionAssignmentRequest1 = new PartitionAssignmentRequest(
                rand.nextLong(), Arrays.asList(rand.nextInt()), rand.nextBoolean(), rand.nextBoolean());
        PartitionAssignmentRequest partitionAssignmentRequest2 = encodeThenDecode(partitionAssignmentRequest1);
        assertEquals(AdminMessageType.PARTITION_ASSIGNMENT_REQUEST, partitionAssignmentRequest1.type());
        assertTrue(partitionAssignmentRequest1.partitionIds.containsAll(partitionAssignmentRequest2.partitionIds));
        assertEquals(partitionAssignmentRequest1.seqNum, partitionAssignmentRequest2.seqNum);
        assertEquals(partitionAssignmentRequest1.toggled, partitionAssignmentRequest2.toggled);
        assertEquals(partitionAssignmentRequest1.deleteStorageFiles, partitionAssignmentRequest2.deleteStorageFiles);
    }

    @Test
    public void testRecordListRequest() {
        RecordListRequest recordListRequest1 = new RecordListRequest(
                rand.nextLong(), rand.nextInt(), rand.nextLong(), rand.nextInt());
        RecordListRequest recordListRequest2 = encodeThenDecode(recordListRequest1);
        assertEquals(AdminMessageType.RECORD_LIST_REQUEST, recordListRequest1.type());
        assertEquals(recordListRequest1.partitionId, recordListRequest2.partitionId);
        assertEquals(recordListRequest1.seqNum, recordListRequest2.seqNum);
        assertEquals(recordListRequest1.transactionId, recordListRequest2.transactionId);
        assertEquals(recordListRequest1.maxNumRecords, recordListRequest2.maxNumRecords);
    }

    @Test
    public void testRecordListResponse() {
        ReqId reqId = new ReqId(rand.nextLong(), rand.nextLong());
        byte[] randBytes1 = new byte[32];
        byte[] randBytes2 = new byte[32];
        rand.nextBytes(randBytes1);
        rand.nextBytes(randBytes2);
        Record record1 = new Record(rand.nextLong(), reqId, rand.nextInt(), randBytes1, Utils.checksum(randBytes1));
        Record record2 = new Record(rand.nextLong(), reqId, rand.nextInt(), randBytes2, Utils.checksum(randBytes2));
        ArrayList<Record> records = new ArrayList<>(Arrays.asList(record1, record2));
        RecordListResponse recordListResponse1 = new RecordListResponse(rand.nextLong(), rand.nextInt(), records);
        RecordListResponse recordListResponse2 = encodeThenDecode(recordListResponse1);
        assertEquals(AdminMessageType.RECORD_LIST_RESPONSE, recordListResponse1.type());
        assertEquals(recordListResponse1.partitionId, recordListResponse2.partitionId);
        assertEquals(recordListResponse1.seqNum, recordListResponse2.seqNum);
        assertEquals(2, recordListResponse2.records.size());
        assertEquals(record1, recordListResponse2.records.get(0));
        assertEquals(record2, recordListResponse2.records.get(1));
    }

    @Test
    public void testLastSessionInfoRequest() {
        LastSessionInfoRequest lastSessionInfoRequest1 = new LastSessionInfoRequest(rand.nextLong(), rand.nextInt());
        LastSessionInfoRequest lastSessionInfoRequest2 = encodeThenDecode(lastSessionInfoRequest1);
        assertEquals(AdminMessageType.LAST_SESSION_INFO_REQUEST, lastSessionInfoRequest1.type());
        assertEquals(lastSessionInfoRequest1.partitionId, lastSessionInfoRequest2.partitionId);
        assertEquals(lastSessionInfoRequest1.seqNum, lastSessionInfoRequest2.seqNum);
    }

    @Test
    public void testLastSessionInfoResponse() {
        SessionInfo sessionInfo = new SessionInfo(rand.nextLong(), rand.nextLong(), rand.nextLong());
        LastSessionInfoResponse lastSessionInfoResponse1 = new LastSessionInfoResponse(
                rand.nextLong(), rand.nextInt(), sessionInfo);
        LastSessionInfoResponse lastSessionInfoResponse2 = encodeThenDecode(lastSessionInfoResponse1);
        assertEquals(AdminMessageType.LAST_SESSION_INFO_RESPONSE, lastSessionInfoResponse1.type());
        assertEquals(lastSessionInfoResponse1.partitionId, lastSessionInfoResponse2.partitionId);
        assertEquals(lastSessionInfoResponse1.seqNum, lastSessionInfoResponse2.seqNum);
        assertEquals(sessionInfo.lowWaterMark, lastSessionInfoResponse2.lastSessionInfo.lowWaterMark);
        assertEquals(sessionInfo.localLowWaterMark, lastSessionInfoResponse2.lastSessionInfo.localLowWaterMark);
        assertEquals(sessionInfo.sessionId, lastSessionInfoResponse2.lastSessionInfo.sessionId);
    }

    @SuppressWarnings("unchecked")
    private <T extends Message> T encodeThenDecode(T message) {
        ByteArrayMessageAttributeWriter writer = new ByteArrayMessageAttributeWriter();
        codec.encode(message, writer);
        ByteArrayMessageAttributeReader reader = new ByteArrayMessageAttributeReader(writer.toByteArray());
        return (T) codec.decode(reader);
    }
}
