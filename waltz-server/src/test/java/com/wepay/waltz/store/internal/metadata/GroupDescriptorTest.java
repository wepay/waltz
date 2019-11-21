package com.wepay.waltz.store.internal.metadata;

import com.wepay.waltz.common.metadata.GroupDescriptor;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class GroupDescriptorTest {

    private Random rand = new Random();

    @Test
    public void testSerialization() throws Exception {
        final byte numGroups = (byte) (rand.nextInt(10) + 1);
        final int numStorageNodesPerGroup = rand.nextInt(5) + 1;

        Map<String, Integer> groups = new HashMap<>();
        for (int i = 0; i < numStorageNodesPerGroup; i++) {
            String connectStrings = "dummy:" + i;
            int groupId = rand.nextInt(numGroups);
            groups.put(connectStrings, groupId);
        }

        GroupDescriptor groupDescriptor = new GroupDescriptor(groups);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (DataOutputStream out = new DataOutputStream(baos)) {
                groupDescriptor.writeTo(out);
            }

            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
                assertEquals(groupDescriptor, GroupDescriptor.readFrom(in));
            }
        }
    }

}
