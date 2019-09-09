package com.wepay.riff.network;

import com.wepay.riff.util.Logging;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.slf4j.Logger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

public final class ServerSSL {

    private static Logger logger = Logging.getLogger(ServerSSL.class);

    private ServerSSL() {
    }

    public static SslContext createContext(SSLConfig config) throws GeneralSecurityException, IOException {
        String keyStoreLocation = (String) config.getOpt(SSLConfig.KEY_STORE_LOCATION).orElse(null);
        String trustStoreLocation = (String) config.getOpt(SSLConfig.TRUST_STORE_LOCATION).orElse(null);

        try {
            SslContextBuilder builder;

            if (keyStoreLocation != null) {
                String keyStorePassword = (String) config.get(SSLConfig.KEY_STORE_PASSWORD);
                String keyStoreType = (String) config.get(SSLConfig.KEY_STORE_TYPE);
                String keyManagerAlgorithm = (String) config.get(SSLConfig.KEY_MANAGER_ALGORITHM);

                KeyStore keyStore = KeyStore.getInstance(keyStoreType);
                try (FileInputStream is = new FileInputStream(keyStoreLocation)) {
                    keyStore.load(is, keyStorePassword.toCharArray());
                } catch (IOException ex) {
                    logger.error("failed to load the key store: location=" + keyStoreLocation + " type=" + keyStoreType);
                    throw ex;
                }

                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(keyManagerAlgorithm);
                keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());

                builder = SslContextBuilder.forServer(keyManagerFactory);

            } else {
                logger.error("KeyStoreLocation was not specified. Building SSL context using self-signed certificate. This is not suitable for PRODUCTION.");

                final SelfSignedCertificate ssc = new SelfSignedCertificate();
                builder = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey());
            }

            if (trustStoreLocation != null) {
                String trustStorePassword = (String) config.get(SSLConfig.TRUST_STORE_PASSWORD);
                String trustStoreType = (String) config.get(SSLConfig.TRUST_STORE_TYPE);
                String trustManagerAlgorithm = (String) config.get(SSLConfig.TRUST_MANAGER_ALGORITHM);

                KeyStore trustStore = KeyStore.getInstance(trustStoreType);
                try (FileInputStream is = new FileInputStream(trustStoreLocation)) {
                    trustStore.load(is, trustStorePassword.toCharArray());
                } catch (IOException ex) {
                    logger.error("failed to load the trust store: location=" + trustStoreLocation + " type=" + trustStoreType);
                    throw ex;
                }
                TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(trustManagerAlgorithm);
                trustManagerFactory.init(trustStore);

                builder.trustManager(trustManagerFactory).clientAuth(ClientAuth.REQUIRE);

            } else {
                logger.error("TrustStoreLocation was not specified. Building SSL context without client auth. This is not suitable for PRODUCTION.");
            }

            return builder.build();

        } catch (GeneralSecurityException | IOException e) {
            logger.error("Failed to create SslContext", e);
            throw e;
        }
    }

    public static SslContext createInsecureContext() throws GeneralSecurityException, IOException {
        try {
            SslContextBuilder builder;

            logger.error("Building SslContext using self-signed certificate and InsecureTrustManagerFactory. This is not suitable for PRODUCTION.");

            final SelfSignedCertificate ssc = new SelfSignedCertificate();

            builder = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey());

            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);

            return builder.build();

        } catch (Exception e) {
            logger.error("Failed to create SslContext", e);
            throw e;
        }
    }

}
