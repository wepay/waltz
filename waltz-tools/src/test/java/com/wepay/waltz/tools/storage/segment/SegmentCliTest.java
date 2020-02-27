package com.wepay.waltz.tools.storage.segment;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.storage.server.internal.Segment;
import com.wepay.waltz.storage.server.internal.SegmentFileHeader;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.nio.charset.Charset;
import java.util.UUID;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SegmentCliTest {

    private static final String segmentFilePath = "test.seg";
    private static final ReqId someReqId = new ReqId(0L, 0L);
    private static final int someHeader = 0;
    private static final int someChecksum = 0;
    private static final byte[] data0 = "accountId:1000000001\tamount:8\taction:deposit".getBytes(Charset.forName("UTF-8"));
    private static final byte[] data1 = "apple".getBytes(Charset.forName("UTF-8"));
    private static final byte[] data2 = "∂pple".getBytes(Charset.forName("UTF-8")); // contains non-ASCII character

    @Test
    public void testCollectStatistic() throws Exception {
        String[] args = {segmentFilePath};
        SegmentCli.Statistic cli = new SegmentCli.Statistic(args);
        SegmentCli.Statistic spyCli = spy(cli);
        doNothing().when(spyCli).initWithSegmentFilePath(segmentFilePath);

        Segment mockSegment = mock(Segment.class);
        when(mockSegment.getRecord(0L)).thenReturn(new Record(0L, someReqId, someHeader, data0, someChecksum));
        when(mockSegment.getRecord(1L)).thenReturn(new Record(1L, someReqId, someHeader, data1, someChecksum));
        when(mockSegment.getRecord(2L)).thenReturn(new Record(2L, someReqId, someHeader, data2, someChecksum));
        spyCli.segment = mockSegment;

        spyCli.processCmd();
        verify(spyCli, times(1)).collectStatistic();
        Assert.assertEquals(3, spyCli.numOfRows);
        Assert.assertEquals(44, spyCli.maxRowByteSize);
        Assert.assertEquals(5, spyCli.minRowByteSize);
        Assert.assertEquals(18.67, spyCli.avgRowByteSize, 2);
        Assert.assertEquals(7, spyCli.midRowByteSize, 2);
    }

    @Test
    public void verifySingleTransactionShouldSucceed() throws Exception {
        final long retrievedTransactionId = 0L;
        final long expectedTransactionId = 0L;
        final long offset = 1L;
        final int recordSize = 100;

        String[] args = {segmentFilePath, "-t", String.valueOf(expectedTransactionId)};
        SegmentCli.Verify cli = new SegmentCli.Verify(args);
        SegmentCli.Verify spyCli = spy(cli);
        doNothing().when(spyCli).initWithSegmentFilePath(segmentFilePath);

        Segment.Index mockIndex = mock(Segment.Index.class);
        when(mockIndex.get(expectedTransactionId)).thenReturn(offset);
        spyCli.index = mockIndex;

        Segment mockSegment = mock(Segment.class);
        when(mockSegment.getRecord(0L)).thenReturn(new Record(retrievedTransactionId, someReqId, someHeader, data0, someChecksum));
        when(mockSegment.checkRecord(offset, expectedTransactionId)).thenReturn(recordSize);
        spyCli.segment = mockSegment;

        ResultCaptor<Boolean> resultCaptor = new ResultCaptor<>();
        doAnswer(resultCaptor).when(spyCli).verifyTransaction(expectedTransactionId);

        spyCli.processCmd();
        verify(spyCli, times(1)).verifyTransaction(expectedTransactionId);
        Assert.assertTrue(resultCaptor.getResult().booleanValue());
    }

    @Test
    public void verifySingleTransactionShouldFail() throws Exception {
        final long retrievedTransactionId = 1L;
        final long expectedTransactionId = 0L;
        final long offset = 1L;
        final int recordSize = 100;

        String[] args = {segmentFilePath, "-t", String.valueOf(expectedTransactionId)};
        SegmentCli.Verify cli = new SegmentCli.Verify(args);
        SegmentCli.Verify spyCli = spy(cli);
        doNothing().when(spyCli).initWithSegmentFilePath(segmentFilePath);

        Segment.Index mockIndex = mock(Segment.Index.class);
        when(mockIndex.get(expectedTransactionId)).thenReturn(offset);
        spyCli.index = mockIndex;

        Segment mockSegment = mock(Segment.class);
        when(mockSegment.getRecord(0L)).thenReturn(new Record(retrievedTransactionId, someReqId, someHeader, data0, someChecksum));
        when(mockSegment.checkRecord(offset, expectedTransactionId)).thenReturn(recordSize);
        spyCli.segment = mockSegment;

        ResultCaptor<Boolean> resultCaptor = new ResultCaptor<>();
        doAnswer(resultCaptor).when(spyCli).verifyTransaction(expectedTransactionId);

        spyCli.processCmd();
        verify(spyCli, times(1)).verifyTransaction(expectedTransactionId);
        Assert.assertFalse(resultCaptor.getResult().booleanValue());
    }

    @Test
    public void verifyAllTransactionsShouldSucceed() throws Exception {
        final long offset = 1L;
        final int recordSize = 100;

        String[] args = {segmentFilePath};
        SegmentCli.Verify cli = new SegmentCli.Verify(args);
        SegmentCli.Verify spyCli = spy(cli);
        doNothing().when(spyCli).initWithSegmentFilePath(segmentFilePath);

        Segment.Index mockIndex = mock(Segment.Index.class);
        when(mockIndex.get(anyLong())).thenReturn(offset);
        spyCli.index = mockIndex;

        Segment mockSegment = mock(Segment.class);
        when(mockSegment.getRecord(0L)).thenReturn(new Record(0L, someReqId, someHeader, data0, someChecksum));
        when(mockSegment.getRecord(1L)).thenReturn(new Record(1L, someReqId, someHeader, data1, someChecksum));
        when(mockSegment.getRecord(2L)).thenReturn(new Record(2L, someReqId, someHeader, data2, someChecksum));
        when(mockSegment.checkRecord(anyLong(), anyLong())).thenReturn(recordSize);
        spyCli.segment = mockSegment;

        ResultCaptor<Boolean> resultCaptor = new ResultCaptor<>();
        doAnswer(resultCaptor).when(spyCli).verifyTransactions();

        spyCli.processCmd();
        verify(spyCli, times(1)).verifyTransactions();
        verify(mockSegment, times(4)).getRecord(anyLong());
        Assert.assertTrue(resultCaptor.getResult().booleanValue());
    }

    @Test
    public void verifyAllTransactionsShouldFail() throws Exception {
        final long offset = 1L;
        final int recordSize = 100;

        String[] args = {segmentFilePath};
        SegmentCli.Verify cli = new SegmentCli.Verify(args);
        SegmentCli.Verify spyCli = spy(cli);
        doNothing().when(spyCli).initWithSegmentFilePath(segmentFilePath);

        Segment.Index mockIndex = mock(Segment.Index.class);
        when(mockIndex.get(anyLong())).thenReturn(offset);

        spyCli.index = mockIndex;

        Segment mockSegment = mock(Segment.class);
        when(mockSegment.getRecord(0L)).thenReturn(new Record(0L, someReqId, someHeader, data0, someChecksum));
        when(mockSegment.getRecord(1L)).thenReturn(new Record(0L, someReqId, someHeader, data1, someChecksum));
        when(mockSegment.getRecord(2L)).thenReturn(new Record(0L, someReqId, someHeader, data2, someChecksum));
        when(mockSegment.checkRecord(anyLong(), anyLong())).thenReturn(recordSize);
        spyCli.segment = mockSegment;

        ResultCaptor<Boolean> resultCaptor = new ResultCaptor<>();
        doAnswer(resultCaptor).when(spyCli).verifyTransactions();

        spyCli.processCmd();
        verify(spyCli, times(1)).verifyTransactions();
        verify(mockSegment, times(4)).getRecord(anyLong());
        Assert.assertFalse(resultCaptor.getResult().booleanValue());
    }

    public static class ResultCaptor<T> implements Answer {
        private T result = null;
        public T getResult() {
            return result;
        }

        @Override
        @SuppressWarnings("unchecked")
        public T answer(InvocationOnMock invocationOnMock) throws Throwable {
            result = (T) invocationOnMock.callRealMethod();
            return result;
        }
    }

    @Test
    public void testDumpMetadata() throws Exception {
        String[] args = {segmentFilePath, "-m"};

        SegmentCli.DumpSegment cli = new SegmentCli.DumpSegment(args);
        SegmentCli.DumpSegment spyCli = spy(cli);
        doNothing().when(spyCli).initWithSegmentFilePath(segmentFilePath);

        Segment.Index mockIndex = mock(Segment.Index.class);
        spyCli.index = mockIndex;
        spyCli.segment = mock(Segment.class);

        SegmentFileHeader header = new SegmentFileHeader(new UUID(1L, 2L), 3, 4L);
        when(mockIndex.getHeader()).thenReturn(header);

        spyCli.processCmd();
        verify(spyCli, times(1)).dumpHeader();
    }

    @Test
    public void testDumpAllRecords() throws Exception {
        String[] args = {segmentFilePath, "-r"};

        SegmentCli.DumpSegment cli = new SegmentCli.DumpSegment(args);
        SegmentCli.DumpSegment spyCli = spy(cli);
        doNothing().when(spyCli).initWithSegmentFilePath(segmentFilePath);

        SegmentFileHeader header = new SegmentFileHeader(null, 0, 0L);
        Segment.Index mockIndex = mock(Segment.Index.class);
        when(mockIndex.getHeader()).thenReturn(header);
        spyCli.index = mockIndex;

        Segment mockSegment = mock(Segment.class);
        when(mockSegment.getRecord(0L)).thenReturn(new Record(0L, someReqId, someHeader, data0, someChecksum));
        when(mockSegment.getRecord(1L)).thenReturn(new Record(1L, someReqId, someHeader, data1, someChecksum));
        when(mockSegment.getRecord(2L)).thenReturn(new Record(2L, someReqId, someHeader, data2, someChecksum));
        spyCli.segment = mockSegment;

        spyCli.processCmd();
        verify(spyCli, times(1)).dumpRecord();
        verify(mockSegment, times(4)).getRecord(anyLong());
    }

    @Test
    public void testDumpSingleRecord() throws Exception {
        final long transactionId = 5L;
        String[] args = {segmentFilePath, "-r", "-t", String.valueOf(transactionId)};

        SegmentCli.DumpSegment cli = new SegmentCli.DumpSegment(args);
        SegmentCli.DumpSegment spyCli = spy(cli);
        doNothing().when(spyCli).initWithSegmentFilePath(segmentFilePath);

        Segment mockSegment = mock(Segment.class);
        when(mockSegment.getRecord(transactionId)).thenReturn(new Record(0L, someReqId, someHeader, data0, someChecksum));
        spyCli.segment = mockSegment;

        spyCli.processCmd();
        verify(spyCli, times(1)).dumpRecord(transactionId);
        verify(mockSegment, times(1)).getRecord(transactionId);
    }

    @Test
    public void testDumpSingleRecordWithDeserializer() throws Exception {
        final long transactionId = 5L;
        String[] args = {segmentFilePath, "-d", "com.wepay.waltz.tools.storage.segment.DefaultDeserializer", "-r", "-t", String.valueOf(transactionId)};

        SegmentCli.DumpSegment cli = new SegmentCli.DumpSegment(args);
        SegmentCli.DumpSegment spyCli = spy(cli);
        doNothing().when(spyCli).initWithSegmentFilePath(segmentFilePath);

        Segment mockSegment = mock(Segment.class);
        when(mockSegment.getRecord(transactionId)).thenReturn(new Record(0L, someReqId, someHeader, data0, someChecksum));
        spyCli.segment = mockSegment;

        spyCli.processCmd();
        verify(spyCli, times(1)).dumpRecord(transactionId);
        verify(mockSegment, times(1)).getRecord(transactionId);
    }
}
