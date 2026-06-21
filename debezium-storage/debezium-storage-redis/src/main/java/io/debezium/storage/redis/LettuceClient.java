/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

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
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.Range;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * A {@link RedisClient} implementation backed by the <a href="https://github.com/redis/lettuce">Lettuce</a>
 * driver, used for single (standalone) Redis instances.
 * <p>
 * Lettuce fixes the key/value codec per connection, whereas the {@link RedisClient} contract mixes
 * {@code String}-based commands with a single {@code byte[]}-based {@link #hset(byte[], byte[], byte[])}.
 * For that reason this client holds two connections: a UTF-8 string connection for the stream/hash read
 * commands and a byte-array connection dedicated to {@code hset}.
 */
public class LettuceClient implements RedisClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(LettuceClient.class);

    private static final String LOADING_MESSAGE = "LOADING";

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

    @Override
    public void disconnect() {
        close();
    }

    @Override
    public void close() {
        tryErrors(() -> {
            stringConnection.close();
            byteConnection.close();
            client.shutdown();
        });
    }

    @Override
    public String xadd(String key, Map<String, String> hash) {
        return tryErrors(() -> commands.xadd(key, hash));
    }

    @Override
    public List<String> xadd(List<SimpleEntry<String, Map<String, String>>> hashes) {
        return tryErrors(() -> {
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
                LettuceFutures.awaitAll(commandTimeoutMs, TimeUnit.MILLISECONDS, futures.toArray(new RedisFuture[0]));

                List<String> ids = new ArrayList<>(futures.size());
                for (RedisFuture<String> future : futures) {
                    ids.add(future.get());
                }
                return ids;
            }
            catch (ExecutionException ee) {
                Throwable cause = ee.getCause();
                // When Redis is starting, an error with this message is returned.
                // We will retry communicating with the target DB as once Redis is available, this message will be gone.
                if (cause != null && cause.getMessage() != null && cause.getMessage().contains(LOADING_MESSAGE)) {
                    LOGGER.error("Redis is starting", cause);
                }
                else {
                    LOGGER.error("Unexpected error during pipelined xadd", ee);
                    throw new DebeziumException(ee);
                }
            }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new DebeziumException(ie);
            }
            finally {
                async.setAutoFlushCommands(true);
            }
            return Collections.emptyList();
        });
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
        catch (RedisException e) {
            throw new RedisClientConnectionException(e);
        }
    }
}
