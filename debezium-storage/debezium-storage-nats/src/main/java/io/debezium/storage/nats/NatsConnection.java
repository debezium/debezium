/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * Utility class for managing NATS connections and JetStream resources.
 *
 * @author Nick Babcock
 */
public class NatsConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(NatsConnection.class);

    private static final ConcurrentMap<String, NatsConnection> instances = new ConcurrentHashMap<>();

    /**
     * Guards the {@link #instances} cache and the per-instance refcount so
     * that {@link #getInstance} and {@link #close} are atomic with respect to
     * each other. Without it, a close() racing with getInstance() could
     * remove an instance from the cache while another thread is acquiring it.
     */
    private static final Object LOCK = new Object();

    private final NatsCommonConfig config;
    private Connection connection;
    private JetStream jetStream;
    private final AtomicInteger refCount = new AtomicInteger(0);
    private final String cacheKey;

    private JetStreamManagement jetStreamManagement;

    private NatsConnection(NatsCommonConfig config, String cacheKey) {
        this.config = config;
        this.cacheKey = cacheKey;
    }

    public static NatsConnection getInstance(NatsCommonConfig config, String scopeKey) {
        // Build a cache key from URL, credentials, TLS settings and a
        // non-configurable scope identifier so offset and schema users can
        // have independent lifecycles even on the same URL, and connections
        // with different credentials or TLS settings are never shared.
        String key = config.getNatsUrl() + "|" + config.getUser() + "|" + config.getToken() + "|"
                + config.isTlsEnabled() + "|" + config.getTlsTruststorePath() + "|" + config.getTlsKeystorePath() + "|"
                + (scopeKey == null ? "default" : scopeKey);
        synchronized (LOCK) {
            NatsConnection instance = instances.computeIfAbsent(key, k -> new NatsConnection(config, key));
            instance.refCount.incrementAndGet();
            return instance;
        }
    }

    public synchronized Connection getConnection() throws IOException, InterruptedException {
        if (connection == null || connection.getStatus() != Connection.Status.CONNECTED) {
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

        // Ensure the backing stream exists and JetStream has registered its subjects
        try {
            JetStreamManagement jsm = conn.jetStreamManagement();
            String streamName = "OBJ_" + bucketName;
            io.nats.client.api.StreamInfo si = jsm.getStreamInfo(streamName); // throws if not present yet
            LOGGER.debug("Created ObjectStore backing stream: {}, subjects={}", streamName,
                    si.getConfiguration().getSubjects());
        }
        catch (Exception ex) {
            // A brief delay and re-probe; creation is async on some server versions
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            try {
                conn.jetStreamManagement().getStreamInfo("OBJ_" + bucketName);
            }
            catch (Exception ex2) {
                LOGGER.debug("ObjectStore backing stream not immediately visible for bucket '{}'", bucketName, ex2);
            }
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

        if (config.getUser() != null && !config.getUser().isEmpty()) {
            optionsBuilder.userInfo(config.getUser(), config.getPassword());
        }
        if (config.getToken() != null && !config.getToken().isEmpty()) {
            optionsBuilder.token(config.getToken());
        }
        if (config.isTlsEnabled()) {
            try {
                optionsBuilder.secure();
            }
            catch (java.security.NoSuchAlgorithmException e) {
                throw new RuntimeException("Failed to enable TLS for NATS connection", e);
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

        // Wait for JetStream to be responsive to avoid race conditions right after
        // server start.
        // We ping the JetStream management API with retries for a short period.
        long deadlineMs = System.currentTimeMillis() + Math.max(2000, (int) config.getReconnectWait().toMillis());
        Exception lastJsException = null;
        while (System.currentTimeMillis() < deadlineMs) {
            try {
                JetStreamManagement jsm = connection.jetStreamManagement();
                // Probe by requesting a non-existent stream; a JetStreamApiException
                // indicates JS responded and is therefore ready.
                try {
                    jsm.getStreamInfo("_dbz_js_probe_nonexistent_");
                }
                catch (JetStreamApiException expected) {
                    // Expected since the stream doesn't exist; JS is responsive.
                }
                jetStreamManagement = jsm;
                jetStream = connection.jetStream();
                lastJsException = null;
                break;
            }
            catch (Exception e) { // JetStream not ready yet (e.g., 503 No Responders)
                lastJsException = e;
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw ie;
                }
            }
        }
        if (lastJsException != null) {
            // Surface a clear error if JS never became ready within the wait window
            throw new IOException("JetStream management API not ready after connection", lastJsException);
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
        if ((truststorePath == null || truststorePath.isEmpty())
                && (keystorePath == null || keystorePath.isEmpty())) {
            return null;
        }

        try {
            KeyStore trustStore = null;
            if (truststorePath != null && !truststorePath.isEmpty()) {
                trustStore = KeyStore.getInstance(config.getTlsTruststoreType());
                try (InputStream in = new FileInputStream(truststorePath)) {
                    trustStore.load(in, config.getTlsTruststorePassword().toCharArray());
                }
            }

            KeyStore keyStore = null;
            if (keystorePath != null && !keystorePath.isEmpty()) {
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
            throw new RuntimeException("Failed to build SSL context for NATS connection", e);
        }
    }

    public void close() {
        synchronized (LOCK) {
            // Decrement reference count; only close the underlying connection when
            // there are no more users of this shared instance.
            int remaining = refCount.decrementAndGet();
            if (remaining > 0) {
                LOGGER.debug("NATS connection release: {} remaining users for URL {}", remaining, config.getNatsUrl());
                return;
            }

            if (remaining < 0) {
                // Guard against accidental extra close() calls
                refCount.compareAndSet(remaining, 0);
            }

            if (connection != null) {
                try {
                    connection.close();
                    LOGGER.info("NATS connection closed (last user) for URL {}", config.getNatsUrl());
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

            // Remove from cache so a future user can create a fresh instance.
            // This must happen even when the connection was never established,
            // otherwise the cache entry would leak.
            instances.remove(cacheKey, this);
        }
    }

    private void warmUpObjectStore(ObjectStore os) {
        final String key = "__dbz_os_warmup__";
        byte[] payload = new byte[]{ 1 };
        long deadline = System.currentTimeMillis() + 2_000; // up to 2s warm-up
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                os.put(key, new java.io.ByteArrayInputStream(payload));
                try {
                    os.delete(key);
                }
                catch (Exception ignore) {
                }
                return; // success
            }
            catch (Exception e) {
                last = e;
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        if (last != null) {
            LOGGER.debug("ObjectStore warm-up did not confirm readiness: {}", last.toString());
        }
    }

    public boolean isConnected() {
        return connection != null && connection.getStatus() == Connection.Status.CONNECTED;
    }

}
