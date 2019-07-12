package com.wepay.waltz.test.util;

import com.wepay.riff.network.SSLConfig;
import com.wepay.waltz.common.util.Utils;
import io.netty.handler.ssl.util.SelfSignedCertificate;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Properties;

public class SslSetup {

    private static final String PASSWD = "waltz123";

    private final KeyStoreManager keyStoreMgr;
    private final KeyStoreManager trustStoreMgr;

    private final File dir;

    public SslSetup() throws Exception {
        dir = Files.createTempDirectory("test-").toFile();

        SelfSignedCertificate certificate = new SelfSignedCertificate();

        keyStoreMgr = new KeyStoreManager(new File(dir, "keyStore"), PASSWD)
            .store("key", certificate.key(), certificate.cert()).save();

        trustStoreMgr = new KeyStoreManager(new File(dir, "trustStore"), PASSWD)
            .store("cert", certificate.cert()).save();
    }

    public void close() {
        Utils.removeDirectory(dir);
    }

    public void setConfigParams(Properties props, String prefix) {
        props.setProperty(prefix + SSLConfig.KEY_STORE_PASSWORD, PASSWD);
        props.setProperty(prefix + SSLConfig.KEY_STORE_LOCATION, keyStoreMgr.path());
        props.setProperty(prefix + SSLConfig.TRUST_STORE_PASSWORD, PASSWD);
        props.setProperty(prefix + SSLConfig.TRUST_STORE_LOCATION, trustStoreMgr.path());
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
