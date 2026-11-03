/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import java.io.File;
import java.util.regex.Pattern;

import javax.net.ssl.SSLParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.util.Strings;

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
    private static final String LETTUCE_DRIVER_CLASS = "io.lettuce.core.RedisClient";
    private static final String LETTUCE_DRIVER_ARTIFACT = "io.lettuce:lettuce-core";

    // Package private so that the driver-specific client factories in this package can read the
    // connection settings without this class having to reference their (possibly absent) driver types.
    final String address;
    final int dbIndex;
    final String user;
    final String password;
    final int connectionTimeout;
    final int socketTimeout;
    final boolean sslEnabled;
    final boolean hostnameVerificationEnabled;
    final boolean clusterEnabled;
    final RedisClientLibrary clientLibrary;
    final String truststorePath;
    final String truststorePassword;
    final String truststoreType;
    final String keystorePath;
    final String keystorePassword;
    final String keystoreType;

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
                RedisClientLibrary.JEDIS);
    }

    public RedisConnection(String address, int dbIndex, String user, String password, int connectionTimeout, int socketTimeout, boolean sslEnabled,
                           boolean hostnameVerificationEnabled, String truststorePath, String truststorePassword, String truststoreType,
                           String keystorePath, String keystorePassword, String keystoreType, boolean clusterEnabled, RedisClientLibrary clientLibrary) {
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
        if (RedisClientLibrary.LETTUCE == this.clientLibrary) {
            return createLettuceClient(clientName, waitEnabled, waitTimeout, waitRetry, waitRetryDelay);
        }
        return createJedisClient(clientName, waitEnabled, waitTimeout, waitRetry, waitRetryDelay);
    }

    /**
     * Creates a Lettuce-backed client. {@code lettuce-core} is a provided dependency of this module, so
     * every Lettuce type stays inside {@link LettuceClient}; this method must not reference any of them
     * directly, otherwise loading this class would fail when the driver is absent.
     */
    private RedisClient createLettuceClient(String clientName, boolean waitEnabled, long waitTimeout, boolean waitRetry, long waitRetryDelay) {
        if (this.clusterEnabled) {
            throw new DebeziumException(
                    "The Lettuce client does not yet support Redis Cluster mode; use the Jedis client (redis.client.library=jedis) for cluster mode.");
        }

        requireLettuceDriver(getClass().getClassLoader());

        RedisClient lettuce = LettuceClient.create(this, clientName);
        // Use WAIT wrapper only for standalone
        RedisClient redisClient = waitEnabled ? new WaitReplicasRedisClient(lettuce, 1, waitTimeout, waitRetry, waitRetryDelay) : lettuce;
        LOGGER.info("Using Redis client '{}'", redisClient);
        return redisClient;
    }

    /**
     * Fails fast with an actionable message when the Lettuce driver was requested but is not on the
     * classpath, instead of letting a {@link NoClassDefFoundError} escape from deep inside
     * {@link LettuceClient}. The distribution archive deliberately does not bundle the driver.
     *
     * <p>Package private, and taking the class loader as an argument, so that a test can supply one that
     * hides the driver.
     */
    static void requireLettuceDriver(ClassLoader classLoader) {
        try {
            Class.forName(LETTUCE_DRIVER_CLASS, false, classLoader);
        }
        catch (ClassNotFoundException e) {
            throw new DebeziumException("redis.client.library=lettuce requires '" + LETTUCE_DRIVER_ARTIFACT
                    + "' on the runtime classpath, which the Debezium distribution does not bundle. Add it together with its "
                    + "Netty and Reactor dependencies, or set redis.client.library=jedis.", e);
        }
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
