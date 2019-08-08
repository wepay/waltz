package com.wepay.waltz.storage.server.internal;

import com.wepay.waltz.storage.common.message.admin.AdminFailureResponse;
import com.wepay.waltz.storage.common.message.admin.LastSessionInfoRequest;
import com.wepay.waltz.storage.common.message.admin.RecordListRequest;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Random;

import static org.junit.Assert.assertTrue;

public class AdminServerHandlerTest {

    private static final int NUM_PARTITION = 3;
    private StorageManager storageManager = Mockito.mock(StorageManager.class);
    private AdminServerHandler adminServerHandler = Mockito.spy(new AdminServerHandler(storageManager));
    private ArgumentCaptor<AdminFailureResponse> msgArg = ArgumentCaptor.forClass(AdminFailureResponse.class);
    private ArgumentCaptor<Boolean> boolArg = ArgumentCaptor.forClass(Boolean.class);

    @Test
    public void testRecordList() throws Exception {
        int partitionId = new Random().nextInt(NUM_PARTITION);
        RecordListRequest msg = new RecordListRequest(-1, partitionId, -1, -1);

        adminServerHandler.process(msg);
        Mockito.verify(adminServerHandler).sendMessage(msgArg.capture(), boolArg.capture());
        assertTrue(msgArg.getValue().exception.toString().contains("Partition:" + partitionId + " is not assigned."));
    }

    @Test
    public void testLastSessionInfo() throws Exception {
        int partitionId = new Random().nextInt(NUM_PARTITION);
        LastSessionInfoRequest msg = new LastSessionInfoRequest(-1, partitionId);

        adminServerHandler.process(msg);
        Mockito.verify(adminServerHandler).sendMessage(msgArg.capture(), boolArg.capture());
        assertTrue(msgArg.getValue().exception.toString().contains("Partition:" + partitionId + " is not assigned."));
    }
}
