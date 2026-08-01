/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import java.io.File;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.util.Strings;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.Range;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.SslOptions;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;

/**
 * A {@link RedisClient} implementation backed by the <a href="https://github.com/redis/lettuce">Lettuce</a>
 * driver, used for single (standalone) Redis instances.
 * <p>
 * Lettuce fixes the key/value codec per connection, whereas the {@link RedisClient} contract mixes
 * {@code String}-based commands with a single {@code byte[]}-based {@link #hset(byte[], byte[], byte[])}.
 * For that reason this client holds two connections: a UTF-8 string connection for the stream/hash read
 * commands and a byte-array connection dedicated to {@code hset}.
 * <p>
 * {@code lettuce-core} is an optional dependency of this module, so this class is the only place that
 * references Lettuce types, including the connection setup in {@link #create(RedisConnection, String)}.
 */
public class LettuceClient implements RedisClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(LettuceClient.class);

    private static final String LOADING_MESSAGE = "LOADING";

    /**
     * Shutdown quiet period; the Lettuce default of 2 seconds would needlessly delay connector shutdown.
     */
    private static final Duration SHUTDOWN_QUIET_PERIOD = Duration.ZERO;
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(2);

    private final io.lettuce.core.RedisClient client;
    private final StatefulRedisConnection<String, String> stringConnection;
    private final StatefulRedisConnection<byte[], byte[]> byteConnection;
    private final RedisCommands<String, String> commands;
    private final RedisCommands<byte[], byte[]> byteCommands;
    private final long commandTimeoutMs;

    public LettuceClient(io.lettuce.core.RedisClient client,
                         StatefulRedisConnection<String, String> stringConnection,
                         StatefulRedisConnection<byte[], byte[]> byteConnection,
                         long commandTimeoutMs) {
        this.client = client;
        this.stringConnection = stringConnection;
        this.byteConnection = byteConnection;
        this.commands = stringConnection.sync();
        this.byteCommands = byteConnection.sync();
        this.commandTimeoutMs = commandTimeoutMs;
    }

    /**
     * Connects to the standalone Redis instance described by the given connection settings.
     *
     * @param conn the connection settings; cluster mode must already have been rejected by the caller
     * @param clientName the name to report through {@code CLIENT SETNAME}
     * @return a connected client
     * @throws RedisClientConnectionException if the connection could not be established
     */
    static RedisClient create(RedisConnection conn, String clientName) {
        // Lettuce standalone connects to a single node; if multiple addresses are provided, use the first.
        String firstAddress = conn.address.split(",")[0].trim();
        if (conn.address.contains(",")) {
            LOGGER.warn("Multiple Redis addresses provided but Lettuce cluster mode is not supported; using the first address: {}", firstAddress);
        }
        int separatorIndex = firstAddress.lastIndexOf(':');
        String host = firstAddress.substring(0, separatorIndex);
        int port = Integer.parseInt(firstAddress.substring(separatorIndex + 1));

        // RedisURI.timeout is Lettuce's *command* timeout, which is the counterpart of the Jedis socket
        // timeout; the connect timeout is configured separately through SocketOptions below.
        RedisURI.Builder uriBuilder = RedisURI.builder()
                .withHost(host)
                .withPort(port)
                .withDatabase(conn.dbIndex)
                .withTimeout(Duration.ofMillis(conn.socketTimeout))
                .withSsl(conn.sslEnabled);

        if (conn.sslEnabled) {
            // Mirror the Jedis behaviour: full hostname verification when enabled, otherwise CA-only verification.
            uriBuilder.withVerifyPeer(conn.hostnameVerificationEnabled ? SslVerifyMode.FULL : SslVerifyMode.CA);
        }

        if (!Strings.isNullOrEmpty(conn.user) && !Strings.isNullOrEmpty(conn.password)) {
            uriBuilder.withAuthentication(conn.user, conn.password.toCharArray());
        }
        else if (!Strings.isNullOrEmpty(conn.password)) {
            uriBuilder.withPassword(conn.password.toCharArray());
        }

        ClientOptions.Builder optionsBuilder = ClientOptions.builder()
                .socketOptions(SocketOptions.builder().connectTimeout(Duration.ofMillis(conn.connectionTimeout)).build());

        if (conn.sslEnabled && (!Strings.isNullOrEmpty(conn.truststorePath) || !Strings.isNullOrEmpty(conn.keystorePath))) {
            // Lettuce derives the store type (JKS/PKCS12) from the file itself, so the *.type properties are not applied here.
            SslOptions.Builder sslBuilder = SslOptions.builder();
            if (!Strings.isNullOrEmpty(conn.truststorePath)) {
                sslBuilder.truststore(new File(conn.truststorePath), conn.truststorePassword);
            }
            if (!Strings.isNullOrEmpty(conn.keystorePath)) {
                char[] ksPassword = !Strings.isNullOrEmpty(conn.keystorePassword) ? conn.keystorePassword.toCharArray() : null;
                sslBuilder.keystore(new File(conn.keystorePath), ksPassword);
            }
            optionsBuilder.sslOptions(sslBuilder.build());
        }

        io.lettuce.core.RedisClient lettuceClient = io.lettuce.core.RedisClient.create(uriBuilder.build());
        lettuceClient.setOptions(optionsBuilder.build());

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

            return new LettuceClient(lettuceClient, stringConnection, byteConnection, conn.socketTimeout);
        }
        catch (RedisException e) {
            lettuceClient.shutdown(SHUTDOWN_QUIET_PERIOD, SHUTDOWN_TIMEOUT);
            throw new RedisClientConnectionException(e);
        }
    }

    @Override
    public void disconnect() {
        close();
    }

    @Override
    public void close() {
        tryErrors(() -> {
            stringConnection.close();
            byteConnection.close();
            client.shutdown(SHUTDOWN_QUIET_PERIOD, SHUTDOWN_TIMEOUT);
        });
    }

    @Override
    public String xadd(String key, Map<String, String> hash) {
        return tryErrors(() -> commands.xadd(key, hash));
    }

    @Override
    public List<String> xadd(List<SimpleEntry<String, Map<String, String>>> hashes) {
        RedisAsyncCommands<String, String> async = stringConnection.async();
        try {
            // Make sure the connection is still alive before pipelining
            // to reduce the chance of ending up with duplicate records
            commands.ping();
            async.setAutoFlushCommands(false);
            List<RedisFuture<String>> futures = new ArrayList<>(hashes.size());
            for (SimpleEntry<String, Map<String, String>> hash : hashes) {
                futures.add(async.xadd(hash.getKey(), hash.getValue()));
            }
            // Write all buffered commands to the transport in a single batch
            async.flushCommands();

            // awaitAll neither cancels the futures nor throws on timeout, and futures produced by the async
            // API carry no timeout of their own, so reading them back after a timeout would block forever.
            if (!LettuceFutures.awaitAll(commandTimeoutMs, TimeUnit.MILLISECONDS, futures.toArray(new RedisFuture[0]))) {
                throw new RedisCommandTimeoutException("Pipelined XADD of " + hashes.size() + " entries did not complete within " + commandTimeoutMs + " ms");
            }

            List<String> ids = new ArrayList<>(futures.size());
            for (RedisFuture<String> future : futures) {
                ids.add(future.get());
            }
            return ids;
        }
        catch (RedisCommandExecutionException e) {
            // When Redis is starting, an error with this message is returned. We will retry communicating
            // with the target DB as once Redis is available, this message will be gone.
            // awaitAll unwraps command failures, so they surface here rather than as an ExecutionException.
            if (e.getMessage() != null && e.getMessage().contains(LOADING_MESSAGE)) {
                LOGGER.error("Redis is starting", e);
                return Collections.emptyList();
            }
            LOGGER.error("Unexpected error during pipelined xadd", e);
            throw new DebeziumException(e);
        }
        catch (RedisException e) {
            throw new RedisClientConnectionException(e);
        }
        catch (ExecutionException e) {
            LOGGER.error("Unexpected error during pipelined xadd", e);
            throw new DebeziumException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DebeziumException(e);
        }
        finally {
            async.setAutoFlushCommands(true);
        }
    }

    @Override
    public List<Map<String, String>> xrange(String key) {
        return tryErrors(() -> commands.xrange(key, Range.unbounded())
                .stream()
                .map(StreamMessage::getBody)
                .collect(Collectors.toList()));
    }

    @Override
    public long xlen(String key) {
        return tryErrors(() -> commands.xlen(key));
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return tryErrors(() -> commands.hgetall(key));
    }

    @Override
    public long hset(byte[] key, byte[] field, byte[] value) {
        // Lettuce returns Boolean (true when a new field was created); the contract expects the
        // number of added fields, matching Jedis semantics.
        return tryErrors(() -> Boolean.TRUE.equals(byteCommands.hset(key, field, value)) ? 1L : 0L);
    }

    @Override
    public long waitReplicas(int replicas, long timeout) {
        return tryErrors(() -> commands.waitForReplication(replicas, timeout));
    }

    @Override
    public String info(String section) {
        return tryErrors(() -> commands.info(section));
    }

    @Override
    public String clientList() {
        return tryErrors(() -> commands.clientList());
    }

    @Override
    public String toString() {
        return "LettuceClient [client=" + client + "]";
    }

    private void tryErrors(Runnable runnable) {
        tryErrors(() -> {
            runnable.run();
            return null;
        });
    }

    private <R> R tryErrors(Supplier<R> supplier) {
        try {
            return supplier.get();
        }
        catch (RedisCommandExecutionException e) {
            // Errors returned by the server (WRONGTYPE, NOAUTH, OOM, ...) are not connection failures and
            // must not trigger the reconnect paths that RedisClientConnectionException drives. JedisClient
            // draws the same line: it maps JedisConnectionException only and lets JedisDataException through.
            throw e;
        }
        catch (RedisException e) {
            // Covers RedisConnectionException, RedisCommandTimeoutException and the plain RedisException
            // that Lettuce raises when dispatching on a closed connection.
            throw new RedisClientConnectionException(e);
        }
    }
}
