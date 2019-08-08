package com.wepay.waltz.storage.common.message;

import com.wepay.riff.message.ByteArrayMessageAttributeReader;
import com.wepay.riff.message.ByteArrayMessageAttributeWriter;
import com.wepay.riff.network.Message;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.common.util.Utils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class StorageMessageCodecV0Test {
    private final StorageMessageCodecV0 codec = new StorageMessageCodecV0();
    private final Random rand = new Random();

    @Test
    public void testCodecVersion() {
        assertEquals(0, codec.version());
    }

    @Test
    public void testOpenRequest() {
        OpenRequest openRequest1 = new OpenRequest(new UUID(rand.nextLong(), rand.nextLong()), rand.nextInt());
        OpenRequest openRequest2 = encodeThenDecode(openRequest1);
        assertEquals(StorageMessageType.OPEN_REQUEST, openRequest2.type());
        assertEquals(openRequest1.type(), openRequest2.type());
        assertEquals(openRequest1.sessionId, openRequest2.sessionId);
        assertEquals(openRequest1.seqNum, openRequest2.seqNum);
        assertEquals(openRequest1.partitionId, openRequest2.partitionId);
        assertEquals(openRequest1.key, openRequest2.key);
        assertEquals(openRequest1.numPartitions, openRequest2.numPartitions);
    }

    @Test
    public void testLastSessionInfoRequest() {
        LastSessionInfoRequest lastSessionInfoRequest1 = new LastSessionInfoRequest(rand.nextLong(), rand.nextLong(), rand.nextInt(), true);
        LastSessionInfoRequest lastSessionInfoRequest2 = encodeThenDecode(lastSessionInfoRequest1);
        assertEquals(StorageMessageType.LAST_SESSION_INFO_REQUEST, lastSessionInfoRequest2.type());
        assertEquals(lastSessionInfoRequest1.type(), lastSessionInfoRequest2.type());
        assertEquals(lastSessionInfoRequest1.sessionId, lastSessionInfoRequest2.sessionId);
        assertEquals(lastSessionInfoRequest1.seqNum, lastSessionInfoRequest2.seqNum);
        assertEquals(lastSessionInfoRequest1.partitionId, lastSessionInfoRequest2.partitionId);
        assertEquals(lastSessionInfoRequest1.usedByOfflineRecovery, lastSessionInfoRequest2.usedByOfflineRecovery);
    }

    @Test
    public void testSetLowWaterMarkRequest() {
        SetLowWaterMarkRequest setLowWaterMarkRequest1 = new SetLowWaterMarkRequest(rand.nextLong(), rand.nextLong(), rand.nextInt(), rand.nextLong(), true);
        SetLowWaterMarkRequest setLowWaterMarkRequest2 = encodeThenDecode(setLowWaterMarkRequest1);
        assertEquals(StorageMessageType.SET_LOW_WATER_MARK_REQUEST, setLowWaterMarkRequest2.type());
        assertEquals(setLowWaterMarkRequest1.type(), setLowWaterMarkRequest2.type());
        assertEquals(setLowWaterMarkRequest1.sessionId, setLowWaterMarkRequest2.sessionId);
        assertEquals(setLowWaterMarkRequest1.seqNum, setLowWaterMarkRequest2.seqNum);
        assertEquals(setLowWaterMarkRequest1.partitionId, setLowWaterMarkRequest2.partitionId);
        assertEquals(setLowWaterMarkRequest1.usedByOfflineRecovery, setLowWaterMarkRequest2.usedByOfflineRecovery);
        assertEquals(setLowWaterMarkRequest1.lowWaterMark, setLowWaterMarkRequest2.lowWaterMark);
    }

    @Test
    public void testTruncateRequest() {
        TruncateRequest truncateRequest1 = new TruncateRequest(rand.nextLong(), rand.nextLong(), rand.nextInt(), rand.nextLong(), true);
        TruncateRequest truncateRequest2 = encodeThenDecode(truncateRequest1);
        assertEquals(StorageMessageType.TRUNCATE_REQUEST, truncateRequest2.type());
        assertEquals(truncateRequest1.type(), truncateRequest2.type());
        assertEquals(truncateRequest1.sessionId, truncateRequest2.sessionId);
        assertEquals(truncateRequest1.seqNum, truncateRequest2.seqNum);
        assertEquals(truncateRequest1.partitionId, truncateRequest2.partitionId);
        assertEquals(truncateRequest1.usedByOfflineRecovery, truncateRequest2.usedByOfflineRecovery);
        assertEquals(truncateRequest1.transactionId, truncateRequest2.transactionId);
    }

    @Test
    public void testAppendRequest() {
        ReqId reqId = new ReqId(rand.nextLong(), rand.nextLong());
        byte[] randBytes1 = new byte[32];
        byte[] randBytes2 = new byte[32];
        rand.nextBytes(randBytes1);
        rand.nextBytes(randBytes2);
        Record record1 = new Record(rand.nextLong(), reqId, rand.nextInt(), randBytes1, Utils.checksum(randBytes1));
        Record record2 = new Record(rand.nextLong(), reqId, rand.nextInt(), randBytes2, Utils.checksum(randBytes2));
        ArrayList<Record> records = new ArrayList<>(Arrays.asList(record1, record2));
        AppendRequest appendRequest1 = new AppendRequest(rand.nextLong(), rand.nextLong(), rand.nextInt(), records, true);
        AppendRequest appendRequest2 = encodeThenDecode(appendRequest1);
        assertEquals(StorageMessageType.APPEND_REQUEST, appendRequest2.type());
        assertEquals(appendRequest1.type(), appendRequest2.type());
        assertEquals(appendRequest1.sessionId, appendRequest2.sessionId);
        assertEquals(appendRequest1.seqNum, appendRequest2.seqNum);
        assertEquals(appendRequest1.partitionId, appendRequest2.partitionId);
        assertEquals(appendRequest1.usedByOfflineRecovery, appendRequest2.usedByOfflineRecovery);
        assertEquals(appendRequest1.records, appendRequest2.records);

    }

    @Test
    public void testMaxTransactionIdRequest() {
        MaxTransactionIdRequest maxTransactionIdRequest1 = new MaxTransactionIdRequest(rand.nextLong(), rand.nextLong(), rand.nextInt(), true);
        MaxTransactionIdRequest maxTransactionIdRequest2 = encodeThenDecode(maxTransactionIdRequest1);
        assertEquals(StorageMessageType.MAX_TRANSACTION_ID_REQUEST, maxTransactionIdRequest2.type());
        assertEquals(maxTransactionIdRequest1.type(), maxTransactionIdRequest2.type());
        assertEquals(maxTransactionIdRequest1.sessionId, maxTransactionIdRequest2.sessionId);
        assertEquals(maxTransactionIdRequest1.seqNum, maxTransactionIdRequest2.seqNum);
        assertEquals(maxTransactionIdRequest1.partitionId, maxTransactionIdRequest2.partitionId);
        assertEquals(maxTransactionIdRequest1.usedByOfflineRecovery, maxTransactionIdRequest2.usedByOfflineRecovery);
    }

    private <T extends Message> T encodeThenDecode(T message) {
        ByteArrayMessageAttributeWriter writer = new ByteArrayMessageAttributeWriter();
        codec.encode(message, writer);
        ByteArrayMessageAttributeReader reader = new ByteArrayMessageAttributeReader(writer.toByteArray());
        return (T) codec.decode(reader);
    }
}
