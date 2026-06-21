/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import java.io.File;
import java.time.Duration;
import java.util.regex.Pattern;

import javax.net.ssl.SSLParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.util.Strings;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.DefaultJedisClientConfig.Builder;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.SslOptions;
import redis.clients.jedis.SslVerifyMode;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Establishes a new connection to Redis
 *
 * @author Yossi Shirizli
 */
public class RedisConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisConnection.class);

    public static final String DEBEZIUM_OFFSETS_CLIENT_NAME = "debezium:offsets";
    public static final String DEBEZIUM_SCHEMA_HISTORY = "debezium:schema_history";
    private static final String HOST_PORT_ERROR = "Invalid host:port format in '<...>.redis.address' property.";

    private final String address;
    private final int dbIndex;
    private final String user;
    private final String password;
    private final int connectionTimeout;
    private final int socketTimeout;
    private final boolean sslEnabled;
    private final boolean hostnameVerificationEnabled;
    private final boolean clusterEnabled;
    private final String clientLibrary;
    private final String truststorePath;
    private final String truststorePassword;
    private final String truststoreType;
    private final String keystorePath;
    private final String keystorePassword;
    private final String keystoreType;

    /**
     *
     * @param address
     * @param user
     * @param password
     * @param connectionTimeout
     * @param socketTimeout
     * @param sslEnabled
     */
    public RedisConnection(String address, int dbIndex, String user, String password, int connectionTimeout, int socketTimeout, boolean sslEnabled) {
        this(address, dbIndex, user, password, connectionTimeout, socketTimeout, sslEnabled, false);
    }

    /**
     *
     * @param address
     * @param user
     * @param password
     * @param connectionTimeout
     * @param socketTimeout
     * @param sslEnabled
     * @param hostnameVerificationEnabled
     */
    public RedisConnection(String address, int dbIndex, String user, String password, int connectionTimeout, int socketTimeout, boolean sslEnabled,
                           boolean hostnameVerificationEnabled) {
        this(address, dbIndex, user, password, connectionTimeout, socketTimeout, sslEnabled, hostnameVerificationEnabled,
                null, null, null, null, null, null);
    }

    /**
     *
     * @param address
     * @param user
     * @param password
     * @param connectionTimeout
     * @param socketTimeout
     * @param sslEnabled
     * @param hostnameVerificationEnabled
     * @param truststorePath
     * @param truststorePassword
     * @param truststoreType
     * @param keystorePath
     * @param keystorePassword
     * @param keystoreType
     */
    public RedisConnection(String address, int dbIndex, String user, String password, int connectionTimeout, int socketTimeout, boolean sslEnabled,
                           boolean hostnameVerificationEnabled, String truststorePath, String truststorePassword, String truststoreType,
                           String keystorePath, String keystorePassword, String keystoreType) {
        this(address, dbIndex, user, password, connectionTimeout, socketTimeout, sslEnabled, hostnameVerificationEnabled,
                truststorePath, truststorePassword, truststoreType, keystorePath, keystorePassword, keystoreType, false);
    }

    public RedisConnection(String address, int dbIndex, String user, String password, int connectionTimeout, int socketTimeout, boolean sslEnabled,
                           boolean hostnameVerificationEnabled, String truststorePath, String truststorePassword, String truststoreType,
                           String keystorePath, String keystorePassword, String keystoreType, boolean clusterEnabled) {
        this(address, dbIndex, user, password, connectionTimeout, socketTimeout, sslEnabled, hostnameVerificationEnabled,
                truststorePath, truststorePassword, truststoreType, keystorePath, keystorePassword, keystoreType, clusterEnabled,
                RedisCommonConfig.CLIENT_LIBRARY_JEDIS);
    }

    public RedisConnection(String address, int dbIndex, String user, String password, int connectionTimeout, int socketTimeout, boolean sslEnabled,
                           boolean hostnameVerificationEnabled, String truststorePath, String truststorePassword, String truststoreType,
                           String keystorePath, String keystorePassword, String keystoreType, boolean clusterEnabled, String clientLibrary) {
        validateHostPort(address);

        this.address = address;
        this.dbIndex = dbIndex;
        this.user = user;
        this.password = password;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;
        this.sslEnabled = sslEnabled;
        this.hostnameVerificationEnabled = hostnameVerificationEnabled;
        this.clusterEnabled = clusterEnabled;
        this.clientLibrary = clientLibrary;
        this.truststorePath = truststorePath;
        this.truststorePassword = truststorePassword;
        this.truststoreType = truststoreType;
        this.keystorePath = keystorePath;
        this.keystorePassword = keystorePassword;
        this.keystoreType = keystoreType;
    }

    /**
     * Creates a new RedisConnection instance from the provided RedisCommonConfig.
     *
     * @param config the RedisCommonConfig containing connection parameters
     * @return a new RedisConnection instance
     */
    public static RedisConnection getInstance(RedisCommonConfig config) {
        return new RedisConnection(
                config.getAddress(),
                config.getDbIndex(),
                config.getUser(),
                config.getPassword(),
                config.getConnectionTimeout(),
                config.getSocketTimeout(),
                config.isSslEnabled(),
                config.isHostnameVerificationEnabled(),
                config.getTruststorePath(),
                config.getTruststorePassword(),
                config.getTruststoreType(),
                config.getKeystorePath(),
                config.getKeystorePassword(),
                config.getKeystoreType(),
                config.isClusterEnabled(),
                config.getClientLibrary());
    }

    /**
     *
     * @param clientName
     * @param waitEnabled
     * @param waitTimeout
     * @param waitRetry
     * @param waitRetryDelay
     * @return
     * @throws RedisClientConnectionException
     */
    public RedisClient getRedisClient(String clientName, boolean waitEnabled, long waitTimeout, boolean waitRetry, long waitRetryDelay) {
        if (waitEnabled && waitTimeout <= 0) {
            throw new DebeziumException("Redis client wait timeout should be positive");
        }

        // Use config-driven driver selection; when 'lettuce' is requested, a Lettuce-backed client is created.
        if (RedisCommonConfig.CLIENT_LIBRARY_LETTUCE.equalsIgnoreCase(this.clientLibrary)) {
            return createLettuceClient(clientName, waitEnabled, waitTimeout, waitRetry, waitRetryDelay);
        }
        return createJedisClient(clientName, waitEnabled, waitTimeout, waitRetry, waitRetryDelay);
    }

    private RedisClient createJedisClient(String clientName, boolean waitEnabled, long waitTimeout, boolean waitRetry, long waitRetryDelay) {
        // Use config-driven cluster mode; when enabled, a JedisCluster client is created.
        boolean isCluster = this.clusterEnabled;

        try {
            Builder configBuilder = DefaultJedisClientConfig.builder()
                    .database(this.dbIndex)
                    .connectionTimeoutMillis(this.connectionTimeout)
                    .socketTimeoutMillis(this.socketTimeout)
                    .ssl(this.sslEnabled);

            boolean configureSslOptions = this.sslEnabled && (!Strings.isNullOrEmpty(this.truststorePath) ||
                    !Strings.isNullOrEmpty(this.keystorePath));

            // The SslOptions in Jedis override the default SSL context if explicitly configured.
            // - When a custom truststore or keystore is provided for the Jedis client, hostname verification
            // must also be configured explicitly through the SslOptions.
            // - If no custom truststore or keystore is provided, hostname verification will rely on the
            // SSLParameters, which use the truststore or keystore specified via system properties.
            if (configureSslOptions) {
                var tsPasswordRaw = !Strings.isNullOrEmpty(truststorePassword) ? truststorePassword.toCharArray() : null;
                var ksPasswordRaw = !Strings.isNullOrEmpty(keystorePassword) ? keystorePassword.toCharArray() : null;
                var sslOptions = SslOptions.builder()
                        .truststore(new File(truststorePath), tsPasswordRaw)
                        .trustStoreType(truststoreType)
                        .keystore(new File(keystorePath), ksPasswordRaw)
                        .keyStoreType(keystoreType)
                        .sslVerifyMode(hostnameVerificationEnabled ? SslVerifyMode.FULL : SslVerifyMode.CA)
                        .build();
                configBuilder.sslOptions(sslOptions);
            }
            else if (hostnameVerificationEnabled) {
                // Enforce strict hostname verification to prevent man-in-the-middle attacks.
                var sslParameters = new SSLParameters();
                sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
                configBuilder.sslParameters(sslParameters);
            }

            if (!Strings.isNullOrEmpty(this.user)) {
                configBuilder = configBuilder.user(this.user);
            }

            if (!Strings.isNullOrEmpty(this.password)) {
                configBuilder = configBuilder.password(this.password);
            }

            if (isCluster) {
                // Build cluster from comma-separated host:port list
                java.util.Set<HostAndPort> nodes = new java.util.HashSet<>();
                for (String part : this.address.split(",")) {
                    nodes.add(HostAndPort.from(part.trim()));
                }
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Redis Cluster mode enabled; nodes: {}", nodes);
                }
                redis.clients.jedis.JedisCluster jedisCluster = new redis.clients.jedis.JedisCluster(nodes, configBuilder.build());
                // Try a simple command to ensure connectivity
                jedisCluster.ping();
                RedisClient clusterClient = new JedisClusterClient(jedisCluster);
                LOGGER.info("Using Redis client '{}'", clusterClient);
                return clusterClient; // Do not wrap with WAIT in cluster mode
            }
            else {
                // If multiple addresses are provided but cluster mode is disabled, use the first entry
                String firstAddress = this.address.split(",")[0].trim();
                if (this.address.contains(",")) {
                    LOGGER.warn("Multiple Redis addresses provided but cluster mode disabled; using the first address: {}", firstAddress);
                }
                HostAndPort address = HostAndPort.from(firstAddress);
                Jedis client = new Jedis(address, configBuilder.build());

                // make sure that client is connected
                client.ping();

                try {
                    client.clientSetname(clientName);
                }
                catch (JedisDataException e) {
                    LOGGER.warn("Failed to set client name", e);
                }

                RedisClient jedisClient = new JedisClient(client);
                // Use WAIT wrapper only for standalone
                RedisClient redisClient = waitEnabled ? new WaitReplicasRedisClient(jedisClient, 1, waitTimeout, waitRetry, waitRetryDelay) : jedisClient;
                LOGGER.info("Using Redis client '{}'", redisClient);
                return redisClient;
            }
        }
        catch (JedisConnectionException e) {
            throw new RedisClientConnectionException(e);
        }
    }

    private RedisClient createLettuceClient(String clientName, boolean waitEnabled, long waitTimeout, boolean waitRetry, long waitRetryDelay) {
        if (this.clusterEnabled) {
            throw new DebeziumException(
                    "The Lettuce client does not yet support Redis Cluster mode; use the Jedis client (redis.client.library=jedis) for cluster mode.");
        }

        // Lettuce standalone connects to a single node; if multiple addresses are provided, use the first.
        String firstAddress = this.address.split(",")[0].trim();
        if (this.address.contains(",")) {
            LOGGER.warn("Multiple Redis addresses provided but Lettuce cluster mode is not supported; using the first address: {}", firstAddress);
        }
        int separatorIndex = firstAddress.lastIndexOf(':');
        String host = firstAddress.substring(0, separatorIndex);
        int port = Integer.parseInt(firstAddress.substring(separatorIndex + 1));

        RedisURI.Builder uriBuilder = RedisURI.builder()
                .withHost(host)
                .withPort(port)
                .withDatabase(this.dbIndex)
                .withTimeout(Duration.ofMillis(this.connectionTimeout))
                .withSsl(this.sslEnabled);

        if (this.sslEnabled) {
            // Mirror the Jedis behaviour: full hostname verification when enabled, otherwise CA-only verification.
            uriBuilder.withVerifyPeer(hostnameVerificationEnabled ? io.lettuce.core.SslVerifyMode.FULL : io.lettuce.core.SslVerifyMode.CA);
        }

        if (!Strings.isNullOrEmpty(this.user) && !Strings.isNullOrEmpty(this.password)) {
            uriBuilder.withAuthentication(this.user, this.password.toCharArray());
        }
        else if (!Strings.isNullOrEmpty(this.password)) {
            uriBuilder.withPassword(this.password.toCharArray());
        }

        io.lettuce.core.RedisClient lettuceClient = io.lettuce.core.RedisClient.create(uriBuilder.build());

        boolean configureSslOptions = this.sslEnabled && (!Strings.isNullOrEmpty(this.truststorePath) || !Strings.isNullOrEmpty(this.keystorePath));
        if (configureSslOptions) {
            // Lettuce derives the store type (JKS/PKCS12) from the file itself, so the *.type properties are not applied here.
            io.lettuce.core.SslOptions.Builder sslBuilder = io.lettuce.core.SslOptions.builder();
            if (!Strings.isNullOrEmpty(this.truststorePath)) {
                sslBuilder.truststore(new File(this.truststorePath), this.truststorePassword);
            }
            if (!Strings.isNullOrEmpty(this.keystorePath)) {
                char[] ksPassword = !Strings.isNullOrEmpty(this.keystorePassword) ? this.keystorePassword.toCharArray() : null;
                sslBuilder.keystore(new File(this.keystorePath), ksPassword);
            }
            lettuceClient.setOptions(ClientOptions.builder().sslOptions(sslBuilder.build()).build());
        }

        try {
            StatefulRedisConnection<String, String> stringConnection = lettuceClient.connect();
            StatefulRedisConnection<byte[], byte[]> byteConnection = lettuceClient.connect(ByteArrayCodec.INSTANCE);

            // make sure that the client is connected
            stringConnection.sync().ping();

            try {
                stringConnection.sync().clientSetname(clientName);
            }
            catch (RedisException e) {
                LOGGER.warn("Failed to set client name", e);
            }

            RedisClient lettuce = new LettuceClient(lettuceClient, stringConnection, byteConnection, this.socketTimeout);
            // Use WAIT wrapper only for standalone
            RedisClient redisClient = waitEnabled ? new WaitReplicasRedisClient(lettuce, 1, waitTimeout, waitRetry, waitRetryDelay) : lettuce;
            LOGGER.info("Using Redis client '{}'", redisClient);
            return redisClient;
        }
        catch (RedisException e) {
            lettuceClient.shutdown();
            throw new RedisClientConnectionException(e);
        }
    }

    private void validateHostPort(String address) {
        Pattern pattern = Pattern.compile("^[\\w.-]+:\\d{1,5}+$");
        // Support comma-separated list of host:port pairs for cluster mode
        String[] parts = address.split(",");
        for (String part : parts) {
            String trimmed = part.trim();
            if (trimmed.isEmpty() || !pattern.matcher(trimmed).matches()) {
                throw new DebeziumException(HOST_PORT_ERROR);
            }
        }
    }
}
