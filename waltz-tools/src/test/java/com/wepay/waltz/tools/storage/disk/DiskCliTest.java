package com.wepay.waltz.tools.storage.disk;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.exception.SubCommandFailedException;
import com.wepay.waltz.storage.server.internal.ControlFile;
import com.wepay.waltz.storage.server.internal.PartitionInfoSnapshot;
import com.wepay.waltz.storage.server.internal.Segment;
import com.wepay.waltz.storage.server.internal.SegmentFileHeader;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DiskCliTest {

    private static final String segmentFilePath = "test.seg";
    private static final String controlFilePath = "waltz-storage.ctl";
    private static final ReqId someReqId = new ReqId(0L, 0L);
    private static final int someHeader = 0;
    private static final int someChecksum = 0;
    private static final byte[] data0 = "accountId:1000000001\tamount:8\taction:deposit".getBytes(Charset.forName("UTF-8"));
    private static final byte[] data1 = "apple".getBytes(Charset.forName("UTF-8"));
    private static final byte[] data2 = "∂pple".getBytes(Charset.forName("UTF-8")); // contains non-ASCII character

    @Test
    public void testCollectStatistic() throws Exception {
        String[] args = {segmentFilePath};
        DiskCli.Statistic cli = new DiskCli.Statistic(args);
        DiskCli.Statistic spyCli = spy(cli);
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
        DiskCli.Verify cli = new DiskCli.Verify(args);
        DiskCli.Verify spyCli = spy(cli);
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
        Assert.assertTrue(resultCaptor.getResult());
    }

    @Test
    public void verifySingleTransactionShouldFail() throws Exception {
        final long retrievedTransactionId = 1L;
        final long expectedTransactionId = 0L;
        final long offset = 1L;
        final int recordSize = 100;

        String[] args = {segmentFilePath, "-t", String.valueOf(expectedTransactionId)};
        DiskCli.Verify cli = new DiskCli.Verify(args);
        DiskCli.Verify spyCli = spy(cli);
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
        Assert.assertFalse(resultCaptor.getResult());
    }

    @Test
    public void verifyAllTransactionsShouldSucceed() throws Exception {
        final long offset = 1L;
        final int recordSize = 100;

        String[] args = {segmentFilePath};
        DiskCli.Verify cli = new DiskCli.Verify(args);
        DiskCli.Verify spyCli = spy(cli);
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
        Assert.assertTrue(resultCaptor.getResult());
    }

    @Test
    public void verifyAllTransactionsShouldFail() throws Exception {
        final long offset = 1L;
        final int recordSize = 100;

        String[] args = {segmentFilePath};
        DiskCli.Verify cli = new DiskCli.Verify(args);
        DiskCli.Verify spyCli = spy(cli);
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
        Assert.assertFalse(resultCaptor.getResult());
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
    public void testDumpSegmentMetadata() throws Exception {
        String[] args = {segmentFilePath, "-m"};

        DiskCli.DumpSegment cli = new DiskCli.DumpSegment(args);
        DiskCli.DumpSegment spyCli = spy(cli);
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
    public void testDumpSegmentAllRecords() throws Exception {
        String[] args = {segmentFilePath, "-r"};

        DiskCli.DumpSegment cli = new DiskCli.DumpSegment(args);
        DiskCli.DumpSegment spyCli = spy(cli);
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
    public void testDumpSegmentSingleRecord() throws Exception {
        final long transactionId = 5L;
        String[] args = {segmentFilePath, "-r", "-t", String.valueOf(transactionId)};

        DiskCli.DumpSegment cli = new DiskCli.DumpSegment(args);
        DiskCli.DumpSegment spyCli = spy(cli);
        doNothing().when(spyCli).initWithSegmentFilePath(segmentFilePath);

        Segment mockSegment = mock(Segment.class);
        when(mockSegment.getRecord(transactionId)).thenReturn(new Record(0L, someReqId, someHeader, data0, someChecksum));
        spyCli.segment = mockSegment;

        spyCli.processCmd();
        verify(spyCli, times(1)).dumpRecord(transactionId);
        verify(mockSegment, times(1)).getRecord(transactionId);
    }

    @Test
    public void testDumpSegmentSingleRecordWithDeserializer() throws Exception {
        final long transactionId = 5L;
        String[] args = {segmentFilePath, "-d", "com.wepay.waltz.tools.storage.disk.DefaultDeserializer", "-r", "-t", String.valueOf(transactionId)};

        DiskCli.DumpSegment cli = new DiskCli.DumpSegment(args);
        DiskCli.DumpSegment spyCli = spy(cli);
        doNothing().when(spyCli).initWithSegmentFilePath(segmentFilePath);

        Segment mockSegment = mock(Segment.class);
        when(mockSegment.getRecord(transactionId)).thenReturn(new Record(0L, someReqId, someHeader, data0, someChecksum));
        spyCli.segment = mockSegment;

        spyCli.processCmd();
        verify(spyCli, times(1)).dumpRecord(transactionId);
        verify(mockSegment, times(1)).getRecord(transactionId);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDumpControlFile() throws Exception {
        final int expectedPartitionId = 1;
        String[] args = {controlFilePath, "-p", String.valueOf(expectedPartitionId)};

        DiskCli.DumpControlFile cli = new DiskCli.DumpControlFile(args);
        DiskCli.DumpControlFile spyCli = spy(cli);
        doNothing().when(spyCli).initWithControlFilePath(controlFilePath);

        final int maxPartitions = 3;

        ControlFile mockControlFile = mock(ControlFile.class);
        when(mockControlFile.getNumPartitions()).thenReturn(maxPartitions);

        PartitionInfoSnapshot expectedPartitionInfoSnapshot =
            new PartitionInfoSnapshot(
                expectedPartitionId,
                10L,
                10L,
                8L,
                5
            );

        doReturn(expectedPartitionInfoSnapshot).when(mockControlFile).getPartitionInfoSnapshot(anyInt());
        spyCli.controlFile = mockControlFile;

        doAnswer(invocation -> {
            List<List<String>> content = (List<List<String>>) invocation.getArguments()[0];
            Assert.assertArrayEquals(
                convertToContentStrings(Collections.singletonList(expectedPartitionInfoSnapshot)).toArray(),
                content.toArray()
            );
            return true;
        }).when(spyCli).dump(anyList());

        spyCli.processCmd();
        verify(mockControlFile, times(1)).getPartitionInfoSnapshot(eq(expectedPartitionId));
        verify(spyCli, times(1)).dump(anyList());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDumpControlFileUnexpectedArgsShouldFail() throws Exception {
        final int expectedPartitionId = 1;
        String[] args = {controlFilePath, "-p", String.valueOf(expectedPartitionId), "-a", "abcd"};

        DiskCli.DumpControlFile cli = new DiskCli.DumpControlFile(args);
        DiskCli.DumpControlFile spyCli = spy(cli);
        doNothing().when(spyCli).initWithControlFilePath(controlFilePath);

        ControlFile mockControlFile = mock(ControlFile.class);
        spyCli.controlFile = mockControlFile;

        try {
            spyCli.processCmd();
            Assert.fail("Expected a SubCommandFailedException to be thrown");
        } catch (SubCommandFailedException exception) {
            Assert.assertTrue(exception.getMessage().contains("is supported only if dumping for all partitions"));
        }

        verify(mockControlFile, times(0)).getPartitionInfoSnapshot(eq(expectedPartitionId));
        verify(spyCli, times(0)).dump(anyList());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDumpControlFilePartitionIdOutOfBoundsShouldFail() throws Exception {
        final int expectedPartitionId = 3;
        String[] args = {controlFilePath, "-p", String.valueOf(expectedPartitionId)};

        DiskCli.DumpControlFile cli = new DiskCli.DumpControlFile(args);
        DiskCli.DumpControlFile spyCli = spy(cli);
        doNothing().when(spyCli).initWithControlFilePath(controlFilePath);

        ControlFile mockControlFile = mock(ControlFile.class);
        spyCli.controlFile = mockControlFile;

        final int maxPartitions = 3;
        when(mockControlFile.getNumPartitions()).thenReturn(maxPartitions);

        try {
            spyCli.processCmd();
            Assert.fail("Expected a SubCommandFailedException to be thrown");
        } catch (SubCommandFailedException exception) {
            Assert.assertTrue(exception.getMessage().contains("Number of partitions:"));
        }

        verify(mockControlFile, times(0)).getPartitionInfoSnapshot(eq(expectedPartitionId));
        verify(spyCli, times(0)).dump(anyList());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDumpControlFileForAllPartitions() throws Exception {
        String[] args = {controlFilePath};

        DiskCli.DumpControlFile cli = new DiskCli.DumpControlFile(args);
        DiskCli.DumpControlFile spyCli = spy(cli);
        doNothing().when(spyCli).initWithControlFilePath(controlFilePath);

        ControlFile mockControlFile = mock(ControlFile.class);
        spyCli.controlFile = mockControlFile;

        Random random = new Random();
        Set<Integer> partitionIds = IntStream.range(0, 10).boxed().collect(Collectors.toSet());
        List<PartitionInfoSnapshot> partitionInfoSnapshots =
            IntStream.range(0, 10).boxed().map(partitionId ->
                new PartitionInfoSnapshot(
                    partitionId,
                    random.nextInt(Integer.SIZE - 1),
                    random.nextInt(Integer.SIZE - 1),
                    random.nextInt(Integer.SIZE - 1),
                    (partitionId % 2 == 0) ? 1 : 5
                )
            ).collect(Collectors.toList());

        doReturn(partitionIds).when(mockControlFile).getPartitionIds();
        doAnswer(invocation -> partitionInfoSnapshots.get((int) invocation.getArguments()[0]))
            .when(mockControlFile).getPartitionInfoSnapshot(anyInt());

        doAnswer(invocation -> {
            List<List<String>> content = (List<List<String>>) invocation.getArguments()[0];
            Assert.assertArrayEquals(convertToContentStrings(partitionInfoSnapshots).toArray(), content.toArray());
            return true;
        }).when(spyCli).dump(anyList());

        spyCli.processCmd();

        verify(mockControlFile, times(1)).getPartitionIds();
        partitionIds.forEach(partitionId ->
            verify(mockControlFile, times(1)).getPartitionInfoSnapshot(eq(partitionId))
        );
        verify(spyCli, times(1)).dump(anyList());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDumpControlFileForAssignedPartitions() throws Exception {
        String[] args = {controlFilePath, "-a", "abdc"};

        DiskCli.DumpControlFile cli = new DiskCli.DumpControlFile(args);
        DiskCli.DumpControlFile spyCli = spy(cli);
        doNothing().when(spyCli).initWithControlFilePath(controlFilePath);

        ControlFile mockControlFile = mock(ControlFile.class);
        spyCli.controlFile = mockControlFile;

        int notAssignedFlags = 1;
        int assignedFlags = 5;

        Random random = new Random();
        Set<Integer> partitionIds = IntStream.range(0, 10).boxed().collect(Collectors.toSet());
        List<PartitionInfoSnapshot> partitionInfoSnapshots =
            IntStream.range(0, 10).boxed().map(partitionId ->
                new PartitionInfoSnapshot(
                    partitionId,
                    random.nextInt(Integer.SIZE - 1),
                    random.nextInt(Integer.SIZE - 1),
                    random.nextInt(Integer.SIZE - 1),
                    (partitionId % 2 == 0) ? notAssignedFlags : assignedFlags
                )
            ).collect(Collectors.toList());

        doReturn(partitionIds).when(mockControlFile).getPartitionIds();
        doAnswer(invocation -> partitionInfoSnapshots.get((int) invocation.getArguments()[0]))
            .when(mockControlFile).getPartitionInfoSnapshot(anyInt());

        doAnswer(invocation -> {
            List<List<String>> content = (List<List<String>>) invocation.getArguments()[0];
            Assert.assertArrayEquals(convertToContentStrings(
                partitionInfoSnapshots.stream().filter(snapshot -> snapshot.isAssigned).collect(Collectors.toList())
            ).toArray(), content.toArray());
            return true;
        }).when(spyCli).dump(anyList());

        spyCli.processCmd();

        verify(mockControlFile, times(1)).getPartitionIds();
        partitionIds.forEach(partitionId ->
            verify(mockControlFile, times(1)).getPartitionInfoSnapshot(eq(partitionId))
        );
        verify(spyCli, times(1)).dump(anyList());
    }

    private List<List<String>> convertToContentStrings(List<PartitionInfoSnapshot> snapshots) {
        return snapshots
            .stream()
            .map(snapshot ->
                Arrays.asList(String.valueOf(snapshot.partitionId), String.valueOf(snapshot.sessionId),
                    String.valueOf(snapshot.lowWaterMark), String.valueOf(snapshot.localLowWaterMark),
                    String.valueOf(snapshot.isAssigned), String.valueOf(snapshot.isAvailable))
            ).collect(Collectors.toList());
    }
}
