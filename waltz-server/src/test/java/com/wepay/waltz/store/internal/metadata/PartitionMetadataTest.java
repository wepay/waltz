package com.wepay.waltz.store.internal.metadata;

import com.wepay.waltz.common.metadata.PartitionMetadata;
import com.wepay.waltz.common.metadata.ReplicaId;
import com.wepay.waltz.common.metadata.ReplicaState;
import com.wepay.waltz.store.internal.TestReplicaSessionManager;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class PartitionMetadataTest {

    private static final long MASK = 0x7FFFFFFFFFFFFFFFL;

    private Random rand = new Random();

    @Test
    public void testSerialization() throws Exception {
        int partitionId = rand.nextInt(Integer.MAX_VALUE);

        HashMap<ReplicaId, ReplicaState> replicaStates = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            ReplicaId replicaId = new ReplicaId(partitionId, String.format(TestReplicaSessionManager.CONNECT_STRING_TEMPLATE, i));
            replicaStates.put(replicaId, new ReplicaState(replicaId, randomPositiveLong(), randomPositiveLong()));
        }
        PartitionMetadata partitionMetadata =
            new PartitionMetadata(rand.nextInt(Integer.MAX_VALUE), randomPositiveLong(), replicaStates);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (DataOutputStream out = new DataOutputStream(baos)) {
                partitionMetadata.writeTo(out);
            }
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
                assertEquals(partitionMetadata, PartitionMetadata.readFrom(in));
            }
        }
    }

    private long randomPositiveLong() {
        return rand.nextLong() & MASK;
    }

}
