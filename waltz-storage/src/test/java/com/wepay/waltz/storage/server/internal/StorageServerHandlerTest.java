package com.wepay.waltz.storage.server.internal;

import com.wepay.waltz.storage.common.message.AppendRequest;
import com.wepay.waltz.storage.common.message.FailureResponse;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.ArgumentCaptor;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class StorageServerHandlerTest {
    private static final int NUM_PARTITION = 3;

    @Test
    public void testPartitions() throws Exception {
        StorageManager storageManager = Mockito.mock(StorageManager.class);
        StorageServerHandler storageServerHandler = Mockito.spy(new StorageServerHandler(storageManager));
        ArgumentCaptor<FailureResponse> msgArg = ArgumentCaptor.forClass(FailureResponse.class);
        ArgumentCaptor<Boolean> boolArg = ArgumentCaptor.forClass(Boolean.class);

        int partitionId = new Random().nextInt(NUM_PARTITION);
        AppendRequest msg = new AppendRequest(-1, -1, partitionId, null);

        storageServerHandler.process(msg);
        Mockito.verify(storageServerHandler).sendMessage(msgArg.capture(), boolArg.capture());
        assertTrue(msgArg.getValue().exception.toString().contains("Partition:" + partitionId + " is not assigned."));
    }
}
