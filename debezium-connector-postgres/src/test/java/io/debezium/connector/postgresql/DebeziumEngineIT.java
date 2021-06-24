/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static org.fest.assertions.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.fest.assertions.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.doc.FixFor;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.CompletionCallback;
import io.debezium.engine.format.Avro;
import io.debezium.engine.format.CloudEvents;
import io.debezium.engine.format.Json;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.SkipWhenKafkaVersion;
import io.debezium.junit.SkipWhenKafkaVersion.KafkaVersion;
import io.debezium.util.LoggingContext;
import io.debezium.util.Testing;

/**
 * Integration tests for Debezium Engine API
 *
 * @author Jiri Pechanec
 */
public class DebeziumEngineIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumEngineIT.class);

    protected static final Path OFFSET_STORE_PATH = Testing.Files.createTestingPath("connector-offsets.txt").toAbsolutePath();

    @Rule
    public SkipTestRule skipTest = new SkipTestRule();

    @Before
    public void before() throws SQLException {
        OFFSET_STORE_PATH.getParent().toFile().mkdirs();
        OFFSET_STORE_PATH.toFile().delete();
        TestHelper.dropAllSchemas();
        TestHelper.execute(
                "CREATE SCHEMA engine;",
                "CREATE TABLE engine.test (id INT PRIMARY KEY, val VARCHAR(32));",
                "INSERT INTO engine.test VALUES(1, 'value1');");
    }

    @Test
    @FixFor("DBZ-1807")
    public void shouldSerializeToJson() throws Exception {
        final Properties props = new Properties();
        props.putAll(TestHelper.defaultConfig().build().asMap());
        props.setProperty("name", "debezium-engine");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty("offset.flush.interval.ms", "0");
        props.setProperty("converter.schemas.enable", "false");

        CountDownLatch allLatch = new CountDownLatch(1);

        final ExecutorService executor = Executors.newFixedThreadPool(1);
        try (final DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class).using(props)
                .notifying((records, committer) -> {

                    for (ChangeEvent<String, String> r : records) {
                        Assertions.assertThat(r.key()).isNotNull();
                        Assertions.assertThat(r.value()).isNotNull();
                        try {
                            final Document key = DocumentReader.defaultReader().read(r.key());
                            final Document value = DocumentReader.defaultReader().read(r.value());
                            Assertions.assertThat(key.getInteger("id")).isEqualTo(1);
                            Assertions.assertThat(value.getDocument("after").getInteger("id")).isEqualTo(1);
                            Assertions.assertThat(value.getDocument("after").getString("val")).isEqualTo("value1");
                        }
                        catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                        allLatch.countDown();
                        committer.markProcessed(r);
                    }
                    committer.markBatchFinished();
                }).using(this.getClass().getClassLoader()).build()) {

            executor.execute(() -> {
                LoggingContext.forConnector(getClass().getSimpleName(), "debezium-engine", "engine");
                engine.run();
            });
            allLatch.await(5000, TimeUnit.MILLISECONDS);
            assertThat(allLatch.getCount()).isEqualTo(0);
        }
    }

    @Test
    @FixFor("DBZ-1807")
    @SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
    public void shouldSerializeToAvro() throws Exception {
        final Properties props = new Properties();
        props.putAll(TestHelper.defaultConfig().build().asMap());
        props.setProperty("name", "debezium-engine");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty("offset.flush.interval.ms", "0");
        props.setProperty("converter.schema.registry.url",
                "http://localhost:" + TestHelper.defaultJdbcConfig().getPort());

        CountDownLatch allLatch = new CountDownLatch(1);

        final ExecutorService executor = Executors.newFixedThreadPool(1);
        try (final DebeziumEngine<ChangeEvent<byte[], byte[]>> engine = DebeziumEngine.create(Avro.class).using(props)
                .notifying((records, committer) -> {
                    Assert.fail("Should not be invoked due to serialization error");
                })
                .using(new CompletionCallback() {

                    @Override
                    public void handle(boolean success, String message, Throwable error) {
                        Assertions.assertThat(success).isFalse();
                        Assertions.assertThat(message).contains("Failed to serialize Avro data from topic debezium");
                        allLatch.countDown();
                    }
                })
                .build()) {

            executor.execute(() -> {
                LoggingContext.forConnector(getClass().getSimpleName(), "debezium-engine", "engine");
                engine.run();
            });
            allLatch.await(5000, TimeUnit.MILLISECONDS);
            assertThat(allLatch.getCount()).isEqualTo(0);
        }
    }

    @Test
    @FixFor("DBZ-1807")
    public void shouldSerializeToCloudEvents() throws Exception {
        final Properties props = new Properties();
        props.putAll(TestHelper.defaultConfig().build().asMap());
        props.setProperty("name", "debezium-engine");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty("offset.flush.interval.ms", "0");
        props.setProperty("converter.schemas.enable", "false");

        CountDownLatch allLatch = new CountDownLatch(1);

        final ExecutorService executor = Executors.newFixedThreadPool(1);
        try (final DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class, CloudEvents.class).using(props)
                .notifying((records, committer) -> {

                    for (ChangeEvent<String, String> r : records) {
                        try {
                            final Document key = DocumentReader.defaultReader().read(r.key());
                            Assertions.assertThat(key.getInteger("id")).isEqualTo(1);
                            Assertions.assertThat(r.value()).isNotNull();

                            final Document value = DocumentReader.defaultReader().read(r.value());
                            Assertions.assertThat(value.getString("id")).contains("txId");
                            Assertions.assertThat(value.getDocument("data").getDocument("payload").getDocument("after").getInteger("id")).isEqualTo(1);
                            Assertions.assertThat(value.getDocument("data").getDocument("payload").getDocument("after").getString("val")).isEqualTo("value1");
                        }
                        catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                        allLatch.countDown();
                        committer.markProcessed(r);
                    }
                    committer.markBatchFinished();
                }).using(this.getClass().getClassLoader()).build()) {

            executor.execute(() -> {
                LoggingContext.forConnector(getClass().getSimpleName(), "debezium-engine", "engine");
                engine.run();
            });
            allLatch.await(5000, TimeUnit.MILLISECONDS);
            assertThat(allLatch.getCount()).isEqualTo(0);
        }
    }

    private static final AtomicInteger offsetStoreSetCalls = new AtomicInteger();

    public static class TestOffsetStore extends FileOffsetBackingStore {

        @Override
        public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> keys) {
            LOGGER.info("Get offsets called");
            return super.get(keys);
        }

        @Override
        public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, Callback<Void> callback) {
            LOGGER.info("Set offsets called");
            offsetStoreSetCalls.incrementAndGet();
            return super.set(values, callback);
        }

    }

    @Test
    @FixFor("DBZ-2461")
    public void testOffsetsCommitAfterStop() throws Exception {
        final AtomicReference<Throwable> exception = new AtomicReference<>();
        DebeziumEngine<ChangeEvent<String, String>> engine;

        TestHelper.execute("DROP TABLE IF EXISTS tests;", "CREATE TABLE tests (id SERIAL PRIMARY KEY);");

        final Properties props = new Properties();
        props.putAll(TestHelper.defaultConfig().build().asMap());
        props.setProperty("name", "debezium-engine");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty("offset.flush.interval.ms", "3000");
        props.setProperty("converter.schemas.enable", "false");
        props.setProperty("offset.storage",
                TestOffsetStore.class.getName());

        engine = DebeziumEngine.create(Json.class).using(props).using(new DebeziumEngine.ConnectorCallback() {
            @Override
            public void connectorStarted() {
            }

            @Override
            public void connectorStopped() {
            }
        }).using((success, message, error) -> {
            exception.compareAndSet(null, error);
        }).notifying((records, committer) -> {
            try {

                for (ChangeEvent<String, String> record : records) {
                    committer.markProcessed(record);
                }
                committer.markBatchFinished();
            }
            catch (Exception e) {
                Testing.printError(e);
            }
        }).build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);

        while (offsetStoreSetCalls.get() < 1) {
            TestHelper.execute("INSERT INTO tests VALUES(default)");
        }
        engine.close();

        Assertions.assertThat(offsetStoreSetCalls.get()).isGreaterThanOrEqualTo(1);
        offsetStoreSetCalls.set(0);

        for (int i = 0; i < 100; i++) {
            TestHelper.execute("INSERT INTO tests VALUES(default)");
        }
        executor.execute(engine);
        while (offsetStoreSetCalls.get() < 1) {
            TestHelper.execute("INSERT INTO tests VALUES(default)");
        }
        engine.close();

        Assertions.assertThat(offsetStoreSetCalls.get()).isGreaterThanOrEqualTo(1);
        Assertions.assertThat(exception.get()).isNull();
    }
}
