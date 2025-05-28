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

    private final String address;
    private final int dbIndex;
    private final String user;
    private final String password;
    private final int connectionTimeout;
    private final int socketTimeout;
    private final boolean sslEnabled;
    private final boolean hostnameVerificationEnabled;
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
        validateHostPort(address);

        this.address = address;
        this.dbIndex = dbIndex;
        this.user = user;
        this.password = password;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;
        this.sslEnabled = sslEnabled;
        this.hostnameVerificationEnabled = hostnameVerificationEnabled;
        this.truststorePath = truststorePath;
        this.truststorePassword = truststorePassword;
        this.truststoreType = truststoreType;
        this.keystorePath = keystorePath;
        this.keystorePassword = keystorePassword;
        this.keystoreType = keystoreType;
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

        HostAndPort address = HostAndPort.from(this.address);

        Jedis client;
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

            client = new Jedis(address, configBuilder.build());

            // make sure that client is connected
            client.ping();

            try {
                client.clientSetname(clientName);
            }
            catch (JedisDataException e) {
                LOGGER.warn("Failed to set client name", e);
            }
        }
        catch (JedisConnectionException e) {
            throw new RedisClientConnectionException(e);
        }

        RedisClient jedisClient = new JedisClient(client);

        // we use 1 for number of replicas as in Redis Enterprise there can be only one replica shard
        RedisClient redisClient = waitEnabled ? new WaitReplicasRedisClient(jedisClient, 1, waitTimeout, waitRetry, waitRetryDelay) : jedisClient;

        LOGGER.info("Using Redis client '{}'", redisClient);

        return redisClient;
    }

    private void validateHostPort(String address) {
        Pattern pattern = Pattern.compile("^[\\w.-]+:\\d{1,5}+$");
        if (!pattern.matcher(address).matches()) {
            throw new DebeziumException(HOST_PORT_ERROR);
        }
    }
}
