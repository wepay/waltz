package com.wepay.waltz.common.message;

import com.wepay.riff.message.ByteArrayMessageAttributeReader;
import com.wepay.riff.message.ByteArrayMessageAttributeWriter;
import com.wepay.riff.network.Message;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.exception.RpcException;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MessageCodecV0Test {

    private final MessageCodecV0 codec = new MessageCodecV0();
    private final Random rand = new Random();

    @Test
    public void test() {
        assertEquals(0, codec.version());

        int[] lockRequest = lock();
        int header = rand.nextInt();
        byte[] data;

        data = data();
        AppendRequest appendRequest1 = new AppendRequest(reqId(), rand.nextLong(), lockRequest, new int[0], new int[0], header, data, Utils.checksum(data));
        AppendRequest appendRequest2 = encodeThenDecode(appendRequest1);
        assertEquals(MessageType.APPEND_REQUEST, appendRequest1.type());
        assertEquals(appendRequest1.type(), appendRequest2.type());
        assertEquals(appendRequest1.reqId, appendRequest2.reqId);
        assertEquals(appendRequest1.clientHighWaterMark, appendRequest2.clientHighWaterMark);
        assertTrue(Arrays.equals(appendRequest1.data, appendRequest2.data));

        MountRequest mountRequest1 = new MountRequest(reqId(), rand.nextLong(), rand.nextLong());
        MountRequest mountRequest2 = encodeThenDecode(mountRequest1);
        assertEquals(MessageType.MOUNT_REQUEST, mountRequest1.type());
        assertEquals(mountRequest1.type(), mountRequest2.type());
        assertEquals(mountRequest1.reqId, mountRequest2.reqId);
        assertEquals(mountRequest1.clientHighWaterMark, mountRequest2.clientHighWaterMark);
        assertEquals(mountRequest1.seqNum, mountRequest2.seqNum);

        MountResponse mountResponse1 = new MountResponse(reqId(), rand.nextInt(3));
        MountResponse mountResponse2 = encodeThenDecode(mountResponse1);
        assertEquals(MessageType.MOUNT_RESPONSE, mountResponse1.type());
        assertEquals(mountResponse1.type(), mountResponse2.type());
        assertEquals(mountResponse1.reqId, mountResponse2.reqId);
        assertEquals(mountResponse1.partitionState, mountResponse2.partitionState);

        FeedData feedData1 = new FeedData(reqId(), rand.nextLong(), header);
        FeedData feedData2 = encodeThenDecode(feedData1);
        assertEquals(MessageType.FEED_DATA, feedData1.type());
        assertEquals(feedData1.type(), feedData2.type());
        assertEquals(feedData1.reqId, feedData2.reqId);
        assertEquals(feedData1.transactionId, feedData2.transactionId);
        assertEquals(feedData1.header, feedData2.header);

        FeedRequest feedRequest1 = new FeedRequest(reqId(), rand.nextLong());
        FeedRequest feedRequest2 = encodeThenDecode(feedRequest1);
        assertEquals(MessageType.FEED_REQUEST, feedRequest1.type());
        assertEquals(feedRequest1.type(), feedRequest2.type());
        assertEquals(feedRequest1.reqId, feedRequest2.reqId);
        assertEquals(feedRequest1.clientHighWaterMark, feedRequest2.clientHighWaterMark);

        FeedSuspended feedSuspended1 = new FeedSuspended(reqId());
        FeedSuspended feedSuspended2 = encodeThenDecode(feedSuspended1);
        assertEquals(MessageType.FEED_SUSPENDED, feedSuspended1.type());
        assertEquals(feedSuspended1.type(), feedSuspended2.type());
        assertEquals(feedSuspended1.reqId, feedSuspended2.reqId);

        FlushRequest flushRequest1 = new FlushRequest(reqId());
        FlushRequest flushRequest2 = encodeThenDecode(flushRequest1);
        assertEquals(MessageType.FLUSH_REQUEST, flushRequest1.type());
        assertEquals(flushRequest1.type(), flushRequest2.type());
        assertEquals(flushRequest1.reqId, flushRequest2.reqId);

        FlushResponse flushResponse1 = new FlushResponse(reqId(), rand.nextLong());
        FlushResponse flushResponse2 = encodeThenDecode(flushResponse1);
        assertEquals(MessageType.FLUSH_RESPONSE, flushResponse1.type());
        assertEquals(flushResponse1.type(), flushResponse2.type());
        assertEquals(flushResponse1.reqId, flushResponse2.reqId);
        assertEquals(flushResponse1.transactionId, flushResponse2.transactionId);

        TransactionDataRequest transactionDataRequest1 = new TransactionDataRequest(reqId(), rand.nextLong());
        TransactionDataRequest transactionDataRequest2 = encodeThenDecode(transactionDataRequest1);
        assertEquals(MessageType.TRANSACTION_DATA_REQUEST, transactionDataRequest1.type());
        assertEquals(transactionDataRequest1.type(), transactionDataRequest2.type());
        assertEquals(transactionDataRequest1.reqId, transactionDataRequest2.reqId);
        assertEquals(transactionDataRequest1.transactionId, transactionDataRequest2.transactionId);

        TransactionDataResponse transactionDataResponse1 =
            new TransactionDataResponse(reqId(), rand.nextLong(), data, Utils.checksum(data));
        TransactionDataResponse transactionDataResponse2 = encodeThenDecode(transactionDataResponse1);
        assertEquals(MessageType.TRANSACTION_DATA_RESPONSE, transactionDataResponse1.type());
        assertEquals(transactionDataResponse1.type(), transactionDataResponse2.type());
        assertEquals(transactionDataResponse1.reqId, transactionDataResponse2.reqId);
        assertEquals(transactionDataResponse1.transactionId, transactionDataResponse2.transactionId);
        assertNotNull(transactionDataResponse1.data);
        assertNotNull(transactionDataResponse2.data);
        assertNull(transactionDataResponse1.exception);
        assertNull(transactionDataResponse2.exception);
        assertTrue(Arrays.equals(transactionDataResponse1.data, transactionDataResponse2.data));
        assertEquals(transactionDataResponse1.checksum, transactionDataResponse2.checksum);

        TransactionDataResponse transactionDataResponse3 =
            new TransactionDataResponse(reqId(), rand.nextLong(), new RpcException(Integer.toString(rand.nextInt())));
        TransactionDataResponse transactionDataResponse4 = encodeThenDecode(transactionDataResponse3);
        assertEquals(MessageType.TRANSACTION_DATA_RESPONSE, transactionDataResponse1.type());
        assertEquals(transactionDataResponse3.type(), transactionDataResponse4.type());
        assertEquals(transactionDataResponse3.reqId, transactionDataResponse4.reqId);
        assertEquals(transactionDataResponse3.transactionId, transactionDataResponse4.transactionId);
        assertNull(transactionDataResponse3.data);
        assertNull(transactionDataResponse4.data);
        assertEquals(0, transactionDataResponse3.checksum);
        assertEquals(0, transactionDataResponse4.checksum);
        assertNotNull(transactionDataResponse3.exception);
        assertNotNull(transactionDataResponse4.exception);
        assertEquals(transactionDataResponse3.exception.toString(), transactionDataResponse4.exception.toString());

        HighWaterMarkRequest highWaterMarkRequest1 = new HighWaterMarkRequest(reqId());
        HighWaterMarkRequest highWaterMarkRequest2 = encodeThenDecode(highWaterMarkRequest1);
        assertEquals(MessageType.HIGH_WATER_MARK_REQUEST, highWaterMarkRequest1.type());
        assertEquals(highWaterMarkRequest1.type(), highWaterMarkRequest2.type());
        assertEquals(highWaterMarkRequest1.reqId, highWaterMarkRequest2.reqId);

        HighWaterMarkResponse highWaterMarkResponse1 = new HighWaterMarkResponse(reqId(), rand.nextLong());
        HighWaterMarkResponse highWaterMarkResponse2 = encodeThenDecode(highWaterMarkResponse1);
        assertEquals(MessageType.HIGH_WATER_MARK_RESPONSE, highWaterMarkResponse1.type());
        assertEquals(highWaterMarkResponse1.type(), highWaterMarkResponse2.type());
        assertEquals(highWaterMarkResponse1.reqId, highWaterMarkResponse2.reqId);
        assertEquals(highWaterMarkResponse1.transactionId, highWaterMarkResponse2.transactionId);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupported1() {
        assertEquals(0, codec.version());

        int[] readLocks = {1};
        int header = rand.nextInt();
        byte[] data;

        data = data();
        AppendRequest appendRequest = new AppendRequest(reqId(), rand.nextLong(), new int[0], readLocks, new int[0], header, data, Utils.checksum(data));
        ByteArrayMessageAttributeWriter writer = new ByteArrayMessageAttributeWriter();
        codec.encode(appendRequest, writer);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupported2() {
        assertEquals(0, codec.version());

        int[] appendLocks = {1};
        int header = rand.nextInt();
        byte[] data;

        data = data();
        AppendRequest appendRequest = new AppendRequest(reqId(), rand.nextLong(), new int[0], new int[0], appendLocks, header, data, Utils.checksum(data));
        ByteArrayMessageAttributeWriter writer = new ByteArrayMessageAttributeWriter();
        codec.encode(appendRequest, writer);
    }

    @SuppressWarnings("unchecked")
    private <T extends Message> T encodeThenDecode(T message) {
        ByteArrayMessageAttributeWriter writer = new ByteArrayMessageAttributeWriter();
        codec.encode(message, writer);
        ByteArrayMessageAttributeReader reader = new ByteArrayMessageAttributeReader(writer.toByteArray());
        return (T) codec.decode(reader);
    }

    private ReqId reqId() {
        return new ReqId(rand.nextLong(), rand.nextLong());
    }

    private int[] lock() {
        int n = rand.nextInt(3);
        int[] lock = new int[n];

        for (int i = 0; i < n; i++) {
            lock[i] = rand.nextInt();
        }

        return lock;
    }

    private byte[] data() {
        return Long.toOctalString(rand.nextLong()).getBytes(StandardCharsets.UTF_8);
    }

}
