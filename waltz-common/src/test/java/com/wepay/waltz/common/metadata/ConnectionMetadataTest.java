package com.wepay.waltz.common.metadata;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class ConnectionMetadataTest {

    private Random rand = new Random();

    @Test
    public void testSerialization() throws Exception {
        final int numStorageNodes = rand.nextInt(10) + 1;

        Map<String, Integer> connections = new HashMap<>();
        for (int i = 0; i < numStorageNodes; i++) {
            String connectString = "test:" + i;
            int adminPort = i + 10;
            connections.put(connectString, adminPort);
        }

        ConnectionMetadata connectionMetadata = new ConnectionMetadata(connections);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (DataOutputStream out = new DataOutputStream(baos)) {
                connectionMetadata.writeTo(out);
            }

            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
                assertEquals(connectionMetadata, ConnectionMetadata.readFrom(in));
            }
        }
    }
}
