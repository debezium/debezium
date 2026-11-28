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
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;

/**
 * A {@link RedisClient} implementation backed by the <a href="https://github.com/redis/lettuce">Lettuce</a>
 * driver, used for single (standalone) Redis instances.
 * <p>
 * Lettuce fixes the key/value codec per connection, whereas the {@link RedisClient} contract mixes
 * {@code String}-based commands with a single {@code byte[]}-based {@link #hset(byte[], byte[], byte[])}.
 * This client deliberately uses a <em>single</em> UTF-8 string connection and sends {@code hset} through
 * {@code dispatch} with its arguments as raw bytes, rather than adding a second byte-array connection.
 * <p>
 * A second connection would break {@link #waitReplicas(int, long)}: Redis evaluates {@code WAIT} against
 * the write offset of the <em>calling</em> connection, so a {@code WAIT} issued on a different connection
 * than the preceding write reports success without ever covering that write. Since {@code hset} is the
 * only {@code byte[]} command and {@link WaitReplicasRedisClient} pairs it with {@code WAIT}, both must
 * travel over the same connection.
 * <p>
 * The {@code hset} arguments are not decoded as UTF-8 either: decoding would replace invalid sequences
 * with U+FFFD, so distinct fields could collapse into one, and the original bytes would be lost for good,
 * even for a later switch back to Jedis, which stores them unchanged.
 * <p>
 * This class is <em>not</em> safe for concurrent use by multiple threads: {@link #xadd(List)} toggles
 * auto-flushing, which is a property of the whole connection, so a concurrent command issued on the same
 * client would sit in the buffer until the batch completes. Each store creates its own client instance.
 * <p>
 * {@code lettuce-core} is a provided dependency of this module and is not bundled in the distribution
 * archive, so this class is the only place that references Lettuce types, including the connection setup
 * in {@link #create(RedisConnection, String)}.
 */
public class LettuceClient implements RedisClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(LettuceClient.class);

    private static final String LOADING_MESSAGE = "LOADING";

    /**
     * The error code Redis returns for a write refused because {@code maxmemory} is reached.
     */
    private static final String OOM_ERROR_PREFIX = "OOM ";

    /**
     * Shutdown quiet period; the Lettuce default of 2 seconds would needlessly delay connector shutdown.
     */
    private static final Duration SHUTDOWN_QUIET_PERIOD = Duration.ZERO;
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(2);

    private final io.lettuce.core.RedisClient client;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> commands;
    private final long commandTimeoutMs;

    public LettuceClient(io.lettuce.core.RedisClient client,
                         StatefulRedisConnection<String, String> connection,
                         long commandTimeoutMs) {
        this.client = client;
        this.connection = connection;
        this.commands = connection.sync();
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

        // Auto-reconnect is disabled because a silent reconnect between a write and the WAIT that follows it
        // would run the WAIT on a new physical connection, whose own write offset is not the one of that
        // write, and report the write as replicated when it may not be. With auto-reconnect disabled, Lettuce
        // fails the in-flight commands and rejects new ones once the connection drops, so the caller gets a
        // RedisClientConnectionException and redoes the write together with its WAIT on a fresh client.
        // Jedis behaves the same way, since it never reconnects a connection on its own.
        ClientOptions.Builder optionsBuilder = ClientOptions.builder()
                .autoReconnect(false)
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
            StatefulRedisConnection<String, String> connection = lettuceClient.connect();

            // make sure that the client is connected
            connection.sync().ping();

            try {
                connection.sync().clientSetname(clientName);
            }
            catch (RedisException e) {
                LOGGER.warn("Failed to set client name", e);
            }

            return new LettuceClient(lettuceClient, connection, conn.socketTimeout);
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
            connection.close();
            client.shutdown(SHUTDOWN_QUIET_PERIOD, SHUTDOWN_TIMEOUT);
        });
    }

    @Override
    public String xadd(String key, Map<String, String> hash) {
        return tryErrors(() -> commands.xadd(key, hash));
    }

    @Override
    public List<String> xadd(List<SimpleEntry<String, Map<String, String>>> hashes) {
        RedisAsyncCommands<String, String> async = connection.async();
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

            return awaitInOrder(futures, commandTimeoutMs);
        }
        catch (RedisCommandExecutionException e) {
            // When Redis is starting, an error with this message is returned. We will retry communicating
            // with the target DB as once Redis is available, this message will be gone.
            // awaitInOrder unwraps command failures, so they surface here rather than as an ExecutionException.
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
        // Issued on the same connection as WAIT on purpose, with the arguments passed as raw bytes so that
        // they reach Redis unchanged whatever the codec of the connection; see the class javadoc.
        // HSET replies with the number of fields added, which is what the contract expects.
        final CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8).add(key).add(field).add(value);
        return tryErrors(() -> commands.dispatch(CommandType.HSET, new IntegerOutput<>(StringCodec.UTF8), args));
    }

    /**
     * Collects the results of pipelined {@code XADD} commands, failing as soon as the responses stop arriving.
     * <p>
     * Redis answers a pipeline in order over a single connection, so each response is given
     * {@code stallTimeoutMs} counted from the arrival of the previous one. This mirrors the Jedis pipeline,
     * where the socket timeout applies to each response read: a stalled server is detected within one
     * timeout, however large the batch, while a batch that keeps making progress is never cut short.
     * Futures produced by the async API carry no timeout of their own, so an unbounded wait here would
     * block forever on a stalled server.
     * <p>
     * A write refused because Redis reached {@code maxmemory} does not fail the batch: its error message
     * takes the place of the entry ID, as the Jedis pipeline does. Redis still executes the commands after
     * it, so callers such as the Debezium Server Redis sink commit the entries that were written and retry
     * only the refused ones.
     *
     * @param futures the pending results, in the order the commands were written
     * @param stallTimeoutMs how long to wait for each response, i.e. {@code redis.socket.timeout.ms}
     * @return the entry IDs, or the out-of-memory error message for refused writes, in the order of {@code futures}
     * @throws RedisCommandTimeoutException if a response does not arrive in time
     * @throws RedisException the failure of a command, unwrapped, such as a server error other than out-of-memory
     *         or a lost connection
     */
    static List<String> awaitInOrder(List<? extends RedisFuture<String>> futures, long stallTimeoutMs) throws InterruptedException {
        final List<String> results = new ArrayList<>(futures.size());
        for (int i = 0; i < futures.size(); i++) {
            final RedisFuture<String> future = futures.get(i);
            if (!future.await(stallTimeoutMs, TimeUnit.MILLISECONDS)) {
                throw new RedisCommandTimeoutException("No response to pipelined command " + (i + 1) + " of " + futures.size()
                        + " within " + stallTimeoutMs + " ms of the previous one");
            }
            try {
                results.add(future.get());
            }
            catch (ExecutionException e) {
                if (isOutOfMemory(e.getCause())) {
                    results.add(e.getCause().getMessage());
                    continue;
                }
                // Rethrow driver failures unwrapped, so that server errors and connection failures keep
                // their distinct handling in the caller.
                if (e.getCause() instanceof RedisException) {
                    throw (RedisException) e.getCause();
                }
                throw new DebeziumException(e.getCause());
            }
        }
        return results;
    }

    private static boolean isOutOfMemory(Throwable failure) {
        return failure instanceof RedisCommandExecutionException
                && failure.getMessage() != null
                && failure.getMessage().startsWith(OOM_ERROR_PREFIX);
    }

    @Override
    public long waitReplicas(int replicas, long timeout) {
        // Must run on the connection that issued the preceding write; see the class javadoc.
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
