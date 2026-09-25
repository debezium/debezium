/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.function.ThrowingRunnable;
import io.debezium.util.DelayStrategy;
import io.debezium.util.RetryingRunnable;
import io.debezium.util.Strings;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.ObjectStore;
import io.nats.client.ObjectStoreManagement;
import io.nats.client.Options;
import io.nats.client.api.ObjectStoreConfiguration;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamInfo;

/**
 * Utility class for managing NATS connections and JetStream resources.
 *
 * @author Nick Chomey
 */
public class NatsConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(NatsConnection.class);

    /**
     * Name of the throwaway object that {@link #warmUpObjectStore(ObjectStore)}
     * writes and deletes. Readers that enumerate the object store must skip it,
     * because it is the only object that can legitimately disappear between a
     * listing and a read.
     */
    public static final String OBJECT_STORE_WARMUP_KEY = "__dbz_os_warmup__";

    /**
     * Delay between probe retries (JetStream readiness, object store backing
     * stream visibility and warm-up).
     */
    private static final Duration PROBE_DELAY = Duration.ofMillis(100);

    private final NatsCommonConfig config;
    private final int probeRetries;
    private Connection connection;
    private JetStream jetStream;
    private JetStreamManagement jetStreamManagement;

    public NatsConnection(NatsCommonConfig config) {
        this.config = config;
        // Probe retries are derived from the reconnect wait: the more patient
        // the user is with connection reconnects, the more patient we are with
        // JetStream and object store readiness probes. The default reconnect
        // wait of 2000ms yields 20 retries at 100ms intervals (~2s).
        this.probeRetries = (int) Math.max(2000, config.getReconnectWait().toMillis()) / 100;
    }

    /**
     * Retry budget used for the readiness probes, and available to callers
     * that need to ride out a short transient failure.
     */
    public int getRetryBudget() {
        return probeRetries;
    }

    /**
     * Build a retry loop for a readiness probe, using the shared probe retry
     * budget derived from the reconnect wait.
     */
    private RetryingRunnable<Exception> probeRunnable(ThrowingRunnable<Exception> action) {
        return RetryingRunnable.<Exception> builder()
                .retries(probeRetries)
                .delayStrategy(DelayStrategy.constant(PROBE_DELAY))
                .doRun(action)
                .build();
    }

    public synchronized Connection getConnection() throws IOException, InterruptedException {
        // The jnats client owns reconnection: while the status is CONNECTING,
        // RECONNECTING or DISCONNECTED it is already working on restoring the
        // connection, so calling Nats.connect() again would replace the field
        // and leak a connection that keeps reconnecting in the background.
        // Reconnecting from here is only needed when the client has given up
        // and closed the connection.
        if (connection == null || connection.getStatus() == Connection.Status.CLOSED) {
            connect();
        }
        return connection;
    }

    public synchronized JetStream getJetStream() throws IOException, InterruptedException {
        if (jetStream == null) {
            Connection conn = getConnection();
            jetStream = conn.jetStream();
        }
        return jetStream;
    }

    public synchronized JetStreamManagement getJetStreamManagement() throws IOException, InterruptedException {
        if (jetStreamManagement == null) {
            Connection conn = getConnection();
            jetStreamManagement = conn.jetStreamManagement();
        }
        return jetStreamManagement;
    }

    public ObjectStore getOrCreateObjectStore(String bucketName)
            throws IOException, InterruptedException, JetStreamApiException {
        Connection conn = getConnection(); // Ensure connection is established
        try {
            // Try to get existing object store and verify by checking status
            ObjectStore os = conn.objectStore(bucketName);
            try {
                os.getStatus(); // throws if bucket does not actually exist
                return os;
            }
            catch (Exception statusEx) {
                LOGGER.debug("ObjectStore bucket '{}' not ready or not existing yet: {}", bucketName,
                        statusEx.toString());
                // fall through to create
            }
        }
        catch (Exception e) {
            // will create below
        }

        LOGGER.debug("ObjectStore bucket '{}' does not exist, creating it", bucketName);

        // Create new object store (persisted on disk to avoid ephemeral responder
        // races)
        ObjectStoreConfiguration osConfig = ObjectStoreConfiguration.builder()
                .name(bucketName)
                .storageType(StorageType.File)
                .build();

        ObjectStoreManagement osm = conn.objectStoreManagement();
        osm.create(osConfig);

        // Ensure the backing stream exists and JetStream has registered its
        // subjects. Creation is async on some server versions, so retry the
        // probe briefly rather than sleeping once and giving up.
        try {
            probeRunnable(() -> {
                JetStreamManagement jsm = conn.jetStreamManagement();
                String streamName = "OBJ_" + bucketName;
                StreamInfo si = jsm.getStreamInfo(streamName); // throws if not present yet
                LOGGER.debug("Created ObjectStore backing stream: {}, subjects={}", streamName,
                        si.getConfiguration().getSubjects());
            }).run();
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        catch (Exception ex) {
            LOGGER.debug("ObjectStore backing stream not immediately visible for bucket '{}'", bucketName, ex);
        }

        ObjectStore os = conn.objectStore(bucketName);
        try {
            // Sanity check that the bucket exists and is responsive
            os.getStatus();
            // Warm up with a tiny put/delete to avoid initial 503 No Responders races
            warmUpObjectStore(os);
            LOGGER.debug("Created ObjectStore bucket '{}' successfully", bucketName);
        }
        catch (Exception ex) {
            LOGGER.warn("ObjectStore bucket '{}' creation sanity check failed", bucketName, ex);
        }
        return os;
    }

    private void connect() throws IOException, InterruptedException {
        LOGGER.info("Connecting to NATS server at {}", config.getNatsUrl());

        Options.Builder optionsBuilder = new Options.Builder()
                .server(config.getNatsUrl())
                .connectionTimeout(config.getConnectionTimeout())
                .maxReconnects(config.getMaxReconnects())
                .reconnectWait(config.getReconnectWait());

        if (!Strings.isNullOrBlank(config.getUser())) {
            optionsBuilder.userInfo(config.getUser(), config.getPassword());
        }
        if (!Strings.isNullOrBlank(config.getToken())) {
            optionsBuilder.token(config.getToken());
        }
        if (config.isTlsEnabled()) {
            try {
                optionsBuilder.secure();
            }
            catch (NoSuchAlgorithmException e) {
                throw new DebeziumException("Failed to enable TLS for NATS connection", e);
            }
            SSLContext sslContext = buildSslContext();
            if (sslContext != null) {
                optionsBuilder.sslContext(sslContext);
            }
        }

        connection = Nats.connect(optionsBuilder.build());

        // Reset JetStream instances when reconnecting
        jetStream = null;
        jetStreamManagement = null;

        // Wait for JetStream to be responsive to avoid race conditions right
        // after server start. We ping the JetStream management API with
        // retries for a short period.
        try {
            probeRunnable(() -> {
                JetStreamManagement jsm = connection.jetStreamManagement();
                // Probe by requesting a non-existent stream; a
                // JetStreamApiException indicates JS responded and is
                // therefore ready.
                try {
                    jsm.getStreamInfo("_dbz_js_probe_nonexistent_");
                }
                catch (JetStreamApiException expected) {
                    // Expected since the stream doesn't exist; JS is responsive.
                }
                jetStreamManagement = jsm;
                jetStream = connection.jetStream();
            }).run();
        }
        catch (InterruptedException ie) {
            throw ie;
        }
        catch (Exception e) {
            // Surface a clear error if JS never became ready within the wait window
            throw new IOException("JetStream management API not ready after connection", e);
        }

        LOGGER.info("Successfully connected to NATS server and JetStream is ready");
    }

    /**
     * Build an {@link SSLContext} from the configured trust store and/or key
     * store. Returns {@code null} when neither is configured, in which case
     * the JVM default context is used.
     */
    private SSLContext buildSslContext() {
        String truststorePath = config.getTlsTruststorePath();
        String keystorePath = config.getTlsKeystorePath();
        if (Strings.isNullOrBlank(truststorePath) && Strings.isNullOrBlank(keystorePath)) {
            return null;
        }

        try {
            KeyStore trustStore = null;
            if (!Strings.isNullOrBlank(truststorePath)) {
                trustStore = KeyStore.getInstance(config.getTlsTruststoreType());
                try (InputStream in = new FileInputStream(truststorePath)) {
                    trustStore.load(in, config.getTlsTruststorePassword().toCharArray());
                }
            }

            KeyStore keyStore = null;
            if (!Strings.isNullOrBlank(keystorePath)) {
                keyStore = KeyStore.getInstance(config.getTlsKeystoreType());
                try (InputStream in = new FileInputStream(keystorePath)) {
                    keyStore.load(in, config.getTlsKeystorePassword().toCharArray());
                }
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, config.getTlsKeystorePassword().toCharArray());

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
            return sslContext;
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to build SSL context for NATS connection", e);
        }
    }

    public synchronized void close() {
        if (connection != null) {
            try {
                connection.close();
                LOGGER.info("NATS connection closed for URL {}", config.getNatsUrl());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warn("Interrupted while closing NATS connection", e);
            }
            finally {
                connection = null;
                jetStream = null;
                jetStreamManagement = null;
            }
        }
    }

    private void warmUpObjectStore(ObjectStore os) {
        final String key = OBJECT_STORE_WARMUP_KEY;
        byte[] payload = new byte[]{ 1 };
        try {
            probeRunnable(() -> {
                os.put(key, new ByteArrayInputStream(payload));
                try {
                    os.delete(key);
                }
                catch (Exception ignore) {
                }
            }).run();
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            LOGGER.debug("ObjectStore warm-up did not confirm readiness: {}", e.toString());
        }
    }

    public boolean isConnected() {
        return connection != null && connection.getStatus() == Connection.Status.CONNECTED;
    }

}
