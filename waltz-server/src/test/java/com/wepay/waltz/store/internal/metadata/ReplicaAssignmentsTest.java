package com.wepay.waltz.store.internal.metadata;

import com.wepay.waltz.common.metadata.ReplicaAssignments;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class ReplicaAssignmentsTest {

    private Random rand = new Random();

    @Test
    public void testSerialization() throws Exception {
        final int numPartitions = rand.nextInt(20) + 1;
        final byte numReplicas = (byte) (rand.nextInt(10) + 1);

        HashMap<String, int[]> replicas = new HashMap<>();
        for (byte replicaIndex = 0; replicaIndex < numReplicas; replicaIndex++) {
            String connectString = "dummy:" + replicaIndex;
            int[] partitionIds = new int[numPartitions];
            for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                partitionIds[partitionId] = partitionId;
            }
            replicas.put(connectString, partitionIds);
        }

        ReplicaAssignments assignments = new ReplicaAssignments(replicas);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        assignments.writeTo(out);
        out.close();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);

        assertEquals(assignments, ReplicaAssignments.readFrom(in));
    }

}
