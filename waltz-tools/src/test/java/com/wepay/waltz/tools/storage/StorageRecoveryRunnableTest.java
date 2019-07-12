package com.wepay.waltz.tools.storage;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.storage.client.StorageAdminClient;
import com.wepay.waltz.storage.client.StorageClient;
import com.wepay.waltz.storage.common.SessionInfo;
import com.wepay.waltz.test.util.ClientUtil;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StorageRecoveryRunnableTest {
    @SuppressWarnings("unchecked")
    @Test
    public void testRecoveryAlreadyDone() throws Exception {
        int partitionId = 0;
        long maxTransactionId = 35L;
        long lowWaterMark = 32L;
        long sessionId = 123L;
        SessionInfo sessionInfo = new SessionInfo(sessionId, lowWaterMark);
        CompletableFuture<Object> sessionInfoCompletableFuture = mock(CompletableFuture.class);
        CompletableFuture<Object> maxTransactionIdCompletableFuture = mock(CompletableFuture.class);
        CompletableFuture<Object> lowWatermarkCompletableFuture = mock(CompletableFuture.class);

        StorageAdminClient mockSourceStorageAdminClient = mock(StorageAdminClient.class);
        StorageAdminClient mockDestinationStorageAdminClient = mock(StorageAdminClient.class);
        StorageClient mockDestinationStorageClient = mock(StorageClient.class);

        when(sessionInfoCompletableFuture.get()).thenReturn(sessionInfo);
        when(maxTransactionIdCompletableFuture.get()).thenReturn(maxTransactionId);
        when(lowWatermarkCompletableFuture.get()).thenReturn(lowWaterMark);
        when(mockSourceStorageAdminClient.lastSessionInfo(partitionId)).thenReturn(sessionInfoCompletableFuture);
        when(mockDestinationStorageAdminClient.lastSessionInfo(partitionId)).thenReturn(sessionInfoCompletableFuture);
        when(mockDestinationStorageClient.lastSessionInfo(sessionId, partitionId)).thenReturn(sessionInfoCompletableFuture);
        when(mockDestinationStorageClient.getMaxTransactionId(sessionId, partitionId)).thenReturn(maxTransactionIdCompletableFuture);
        when(mockDestinationStorageClient.truncate(sessionId, partitionId, lowWaterMark)).thenReturn(lowWatermarkCompletableFuture);

        StorageRecoveryRunnable storageRecoveryRunnable = new StorageRecoveryRunnable(mockSourceStorageAdminClient, mockDestinationStorageAdminClient, mockDestinationStorageClient, partitionId);
        storageRecoveryRunnable.run();

        // Verify that getRecordList is never called, thus we never entered the while loop to read records
        verify(mockSourceStorageAdminClient, never()).getRecordList(anyInt(), anyLong(), anyInt());
        verify(mockSourceStorageAdminClient, times(1)).lastSessionInfo(partitionId);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRecovery() throws Exception {
        int partitionId = 0;
        long destinationMaxTransactionId = 15L;
        long destinationLowWaterMark = 10L;
        long sourceLowWaterMark = 31L;
        long sessionId = 123L;
        SessionInfo sourceSessionInfo = new SessionInfo(sessionId, sourceLowWaterMark);
        SessionInfo destinationSessionInfo = new SessionInfo(sessionId, destinationLowWaterMark);
        ArrayList<Record> twentyRecordList = ClientUtil.makeRecords(10L, 30L);
        ArrayList<Record> oneRecordList = ClientUtil.makeRecords(30L, 31L);
        CompletableFuture<Object> sourceSessionInfoCompletableFuture = mock(CompletableFuture.class);
        CompletableFuture<Object> destinationSessionInfoCompletableFuture = mock(CompletableFuture.class);
        CompletableFuture<Object> maxTransactionIdCompletableFuture = mock(CompletableFuture.class);
        CompletableFuture<Object> lowWatermarkCompletableFuture = mock(CompletableFuture.class);
        CompletableFuture<Object> recordList20CompletableFuture = mock(CompletableFuture.class);
        CompletableFuture<Object> recordList1CompletableFuture = mock(CompletableFuture.class);
        CompletableFuture<Object> appendRecordsCompletableFuture = mock(CompletableFuture.class);
        CompletableFuture<Object> setLowWaterMarkCompletableFuture = mock(CompletableFuture.class);

        StorageAdminClient mockSourceStorageAdminClient = mock(StorageAdminClient.class);
        StorageAdminClient mockDestinationStorageAdminClient = mock(StorageAdminClient.class);
        StorageClient mockDestinationStorageClient = mock(StorageClient.class);
        InOrder inOrder = inOrder(mockDestinationStorageClient);

        when(sourceSessionInfoCompletableFuture.get()).thenReturn(sourceSessionInfo);
        when(destinationSessionInfoCompletableFuture.get()).thenReturn(destinationSessionInfo).thenReturn(sourceSessionInfo);
        when(maxTransactionIdCompletableFuture.get()).thenReturn(destinationMaxTransactionId);
        when(lowWatermarkCompletableFuture.get()).thenReturn(destinationLowWaterMark);

        when(mockDestinationStorageAdminClient.lastSessionInfo(partitionId)).thenReturn(destinationSessionInfoCompletableFuture);
        when(mockSourceStorageAdminClient.lastSessionInfo(partitionId)).thenReturn(sourceSessionInfoCompletableFuture);
        when(mockDestinationStorageClient.lastSessionInfo(sessionId, partitionId)).thenReturn(destinationSessionInfoCompletableFuture);
        when(mockDestinationStorageClient.getMaxTransactionId(sessionId, partitionId)).thenReturn(maxTransactionIdCompletableFuture);
        when(mockDestinationStorageClient.truncate(sessionId, partitionId, destinationLowWaterMark)).thenReturn(lowWatermarkCompletableFuture);
        when(mockDestinationStorageClient.appendRecords(anyLong(), anyInt(), any())).thenReturn(appendRecordsCompletableFuture);
        when(mockDestinationStorageClient.setLowWaterMark(anyLong(), anyInt(), anyLong())).thenReturn(setLowWaterMarkCompletableFuture);

        // First time around loop, return 20 records.
        when(mockSourceStorageAdminClient.getRecordList(partitionId, 11L, 20 /* MAX_BATCH_SIZE */))
                .thenReturn(recordList20CompletableFuture);
        when(recordList20CompletableFuture.get()).thenReturn(twentyRecordList);

        // Second time around, return 1 record.
        when(mockSourceStorageAdminClient.getRecordList(partitionId, 31L, 1))
                .thenReturn(recordList1CompletableFuture);
        when(recordList1CompletableFuture.get()).thenReturn(oneRecordList);

        StorageRecoveryRunnable storageRecoveryRunnable = new StorageRecoveryRunnable(mockSourceStorageAdminClient, mockDestinationStorageAdminClient,
                mockDestinationStorageClient, partitionId);
        storageRecoveryRunnable.run();

        // Verify that getRecordList was called exactly twice, and the proper records were appended to the replica connection
        verify(mockSourceStorageAdminClient, times(2)).getRecordList(eq(partitionId), anyLong(), anyInt());
        inOrder.verify(mockDestinationStorageClient, times(1)).appendRecords(eq(sessionId), eq(partitionId), eq(twentyRecordList));
        inOrder.verify(mockDestinationStorageClient, times(1)).setLowWaterMark(eq(sessionId), eq(partitionId), eq(30L));
        inOrder.verify(mockDestinationStorageClient, times(1)).appendRecords(eq(sessionId), eq(partitionId), eq(oneRecordList));
        inOrder.verify(mockDestinationStorageClient, times(1)).setLowWaterMark(eq(sessionId), eq(partitionId), eq(31L));
    }
}
