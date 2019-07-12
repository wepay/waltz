package com.wepay.waltz.server;

import com.wepay.riff.network.ClientSSL;
import com.wepay.riff.network.SSLConfig;
import com.wepay.riff.network.ServerSSL;
import com.wepay.waltz.client.WaltzClient;
import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.waltz.common.message.AbstractMessage;
import com.wepay.waltz.common.message.AppendRequest;
import com.wepay.waltz.common.message.MessageType;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.test.mock.MockClusterManager;
import com.wepay.waltz.test.mock.MockContext;
import com.wepay.waltz.test.mock.MockWaltzClientCallbacks;
import com.wepay.waltz.test.util.PartitionWithMessageBuffering;
import com.wepay.waltz.test.util.WaltzServerRunner;
import com.wepay.zktools.clustermgr.ManagedClient;
import com.wepay.zktools.clustermgr.ManagedServer;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class WaltzSSLTest extends WaltzTestBase {

    private static final int BEGINNING = 0;
    private static final int MOUNT_REQUEST_RECEIVED = 0x01;
    private static final int FEED_REQUEST_RECEIVED = 0x02;
    private static final int TRANSACTION_1_RECEIVED = 0x04;
    private static final int TRANSACTION_2_RECEIVED = 0x08;

    @Test
    public void testClientConnect() throws Exception {
        Random rand = new Random();
        int partitionId = rand.nextInt(NUM_PARTITIONS);

        SelfSignedCertificate serverCertificate = new SelfSignedCertificate();
        SelfSignedCertificate clientCertificate = new SelfSignedCertificate();

        String skPasswd = "sk12345";
        String stPasswd = "st12345";
        String ckPasswd = "ck12345";
        String ctPasswd = "ct12345";

        File tempDir = Files.createTempDirectory("test-").toFile();
        try {
            final MockClusterManager clusterManager = new MockClusterManager(3);

            // Set up keys and certificates
            KeyStoreManager serverKeyStoreMgr = new KeyStoreManager(new File(tempDir, "serverKeyStore"), skPasswd)
                .store("waltz-server", serverCertificate.key(), serverCertificate.cert()).save();

            KeyStoreManager serverTrustStoreMgr = new KeyStoreManager(new File(tempDir, "serverTrustStore"), stPasswd)
                .store("waltz-client", clientCertificate.cert()).save();

            KeyStoreManager clientKeyStoreMgr = new KeyStoreManager(new File(tempDir, "clientKeyStore"), ckPasswd)
                .store("waltz-client", clientCertificate.key(), clientCertificate.cert()).save();

            KeyStoreManager clientTrustStoreMgr = new KeyStoreManager(new File(tempDir, "clientTrustStore"), ctPasswd)
                .store("waltz-server", serverCertificate.cert()).save();

            Map<Object, Object> map = new HashMap<>();
            map.put(WaltzServerConfig.SERVER_SSL_CONFIG_PREFIX + SSLConfig.KEY_STORE_PASSWORD, skPasswd);
            map.put(WaltzServerConfig.SERVER_SSL_CONFIG_PREFIX + SSLConfig.KEY_STORE_LOCATION, serverKeyStoreMgr.path());
            map.put(WaltzServerConfig.SERVER_SSL_CONFIG_PREFIX + SSLConfig.TRUST_STORE_PASSWORD, stPasswd);
            map.put(WaltzServerConfig.SERVER_SSL_CONFIG_PREFIX + SSLConfig.TRUST_STORE_LOCATION, serverTrustStoreMgr.path());

            SslContext serverSslCtx = ServerSSL.createContext(new WaltzServerConfig(map).getSSLConfig());
            WaltzServerRunner waltzServerRunner = getWaltzServerRunner(serverSslCtx, clusterManager);

            MockWaltzClientCallbacks callbacks = new MockWaltzClientCallbacks()
                .setClientHighWaterMark(0, -1L)
                .setClientHighWaterMark(1, -1L)
                .setClientHighWaterMark(2, -1L)
                .setClientHighWaterMark(3, -1L);

            map = new HashMap<>();
            map.put(WaltzClientConfig.CLIENT_SSL_CONFIG_PREFIX + SSLConfig.KEY_STORE_PASSWORD, ckPasswd);
            map.put(WaltzClientConfig.CLIENT_SSL_CONFIG_PREFIX + SSLConfig.KEY_STORE_LOCATION, clientKeyStoreMgr.path());
            map.put(WaltzClientConfig.CLIENT_SSL_CONFIG_PREFIX + SSLConfig.TRUST_STORE_PASSWORD, ctPasswd);
            map.put(WaltzClientConfig.CLIENT_SSL_CONFIG_PREFIX + SSLConfig.TRUST_STORE_LOCATION, clientTrustStoreMgr.path());

            SslContext clientSslCtx = ClientSSL.createContext(new WaltzClientConfig(map).getSSLConfig());
            WaltzClient client = new WaltzTestClient(clientSslCtx, callbacks, clusterManager);

            try {
                waltzServerRunner.startAsync();
                WaltzServer server = waltzServerRunner.awaitStart();

                ManagedServer managedServer = clusterManager.managedServers().iterator().next();
                setPartitions(managedServer);

                assertEquals(Utils.set(0, 1, 2, 3), server.partitions().keySet());

                ManagedClient managedClient = clusterManager.managedClients().iterator().next();
                setEndpoints(managedClient, managedServer, clusterManager);

                PartitionWithMessageBuffering partition = (PartitionWithMessageBuffering) server.partitions().get(partitionId);

                // The execution is blocked until the connection is established.
                MockContext context1 = MockContext.builder().partitionId(partitionId).header(1).data("transaction1").retry(false).build();
                client.submit(context1);

                MockContext context2 = MockContext.builder().partitionId(partitionId).header(2).data("transaction2").retry(false).build();
                client.submit(context2);

                int history = BEGINNING;
                AbstractMessage msg;

                for (int i = 0; i < 4; i++) {
                    msg = (AbstractMessage) partition.nextMessage(TIMEOUT);
                    assertNotNull(msg);

                    assertEquals(client.clientId(), msg.reqId.clientId());
                    assertEquals(99, msg.reqId.generation());
                    assertEquals(partitionId, msg.reqId.partitionId());

                    switch (msg.type()) {
                        case MessageType.MOUNT_REQUEST:
                            assertTrue(history == BEGINNING);
                            history |= MOUNT_REQUEST_RECEIVED;
                            break;

                        case MessageType.FEED_REQUEST:
                            assertTrue((history & MOUNT_REQUEST_RECEIVED) != 0);
                            history |= FEED_REQUEST_RECEIVED;
                            break;

                        case MessageType.APPEND_REQUEST:
                            assertTrue((history & MOUNT_REQUEST_RECEIVED) != 0);

                            String data = new String(((AppendRequest) msg).data, StandardCharsets.UTF_8);
                            if (data.equals("transaction1")) {
                                assertTrue((history & (TRANSACTION_1_RECEIVED | TRANSACTION_2_RECEIVED)) == 0);
                                history |= TRANSACTION_1_RECEIVED;
                            } else if (data.equals("transaction2")) {
                                assertTrue((history & TRANSACTION_1_RECEIVED) != 0);
                                history |= TRANSACTION_2_RECEIVED;
                            }
                            break;

                        default:
                            break;
                    }
                }

            } finally {
                close(client);
                waltzServerRunner.stop();
            }
            assertEquals(0, clusterManager.managedServers().size());

        } finally {
            Utils.removeDirectory(tempDir);
        }
    }

    public static void close(WaltzClient... clients) {
        for (WaltzClient client : clients) {
            try {
                client.close();
            } catch (Throwable ex) {
                // Ignore
            }
        }
    }

    private static class KeyStoreManager {
        private final File file;
        private final char[] passwd;
        private final KeyStore ks;

        KeyStoreManager(File file, String password) throws GeneralSecurityException, IOException {
            this.file = file;
            this.passwd = password.toCharArray();
            this.ks = KeyStore.getInstance(KeyStore.getDefaultType());

            if (file.exists()) {
                throw new IOException("KeyStore already exists");
            } else {
                ks.load(null, password.toCharArray());

                FileOutputStream os = new FileOutputStream(file);
                try {
                    ks.store(os, passwd);
                } finally {
                    os.close();
                }
            }
        }

        KeyStoreManager save() throws GeneralSecurityException, IOException {
            FileOutputStream os = new FileOutputStream(file);
            try {
                ks.store(os, passwd);
            } finally {
                os.close();
            }
            return this;
        }

        KeyStoreManager store(String alias, Key key, Certificate certificate) throws GeneralSecurityException, IOException {
            ks.setKeyEntry(alias, key, passwd, new Certificate[] {certificate});
            return this;
        }

        KeyStoreManager store(String alias, Certificate certificate) throws GeneralSecurityException, IOException {
            ks.setCertificateEntry(alias, certificate);
            return this;
        }

        String path() {
            return file.getPath();
        }
    }

}
