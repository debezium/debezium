/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine.CompletionCallback;
import io.debezium.function.BooleanConsumer;
import io.debezium.util.Testing;

/**
 * An abstract base class for unit testing {@link SourceConnector} implementations using the Debezium {@link EmbeddedEngine}
 * with local file storage.
 * <p>
 * To use this abstract class, simply create a test class that extends it, and add one or more test methods that
 * {@link #start(Class, Configuration) starts the connector} using your connector's custom configuration.
 * Then, your test methods can call {@link #consumeRecords(int, Consumer)} to consume the specified number
 * of records (the supplied function gives you a chance to do something with the record).
 * 
 * @author Randall Hauch
 */
public abstract class AbstractConnectorTest implements Testing {

    protected static final Path OFFSET_STORE_PATH = Testing.Files.createTestingPath("file-connector-offsets.txt").toAbsolutePath();

    private ExecutorService executor;
    private EmbeddedEngine engine;
    private BlockingQueue<SourceRecord> consumedLines;
    protected long pollTimeoutInMs = TimeUnit.SECONDS.toMillis(5);
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private CountDownLatch latch;
    private JsonConverter keyJsonConverter = new JsonConverter();
    private JsonConverter valueJsonConverter = new JsonConverter();
    private JsonDeserializer keyJsonDeserializer = new JsonDeserializer();
    private JsonDeserializer valueJsonDeserializer = new JsonDeserializer();

    @Before
    public final void initializeConnectorTestFramework() {
        keyJsonConverter = new JsonConverter();
        valueJsonConverter = new JsonConverter();
        keyJsonDeserializer = new JsonDeserializer();
        valueJsonDeserializer = new JsonDeserializer();
        Configuration converterConfig = Configuration.create().build();
        Configuration deserializerConfig = Configuration.create().build();
        keyJsonConverter.configure(converterConfig.asMap(), true);
        valueJsonConverter.configure(converterConfig.asMap(), false);
        keyJsonDeserializer.configure(deserializerConfig.asMap(), true);
        valueJsonDeserializer.configure(deserializerConfig.asMap(), false);

        resetBeforeEachTest();
        consumedLines = new ArrayBlockingQueue<>(getMaximumEnqueuedRecordCount());
        Testing.Files.delete(OFFSET_STORE_PATH);
    }

    /**
     * Stop the connector and block until the connector has completely stopped.
     */
    @After
    public final void stopConnector() {
        stopConnector(null);
    }

    /**
     * Stop the connector, and return whether the connector was successfully stopped.
     * 
     * @param callback the function that should be called with whether the connector was successfully stopped; may be null
     */
    public void stopConnector(BooleanConsumer callback) {
        try {
            // Try to stop the connector ...
            if (engine != null && engine.isRunning()) {
                engine.stop();
                try {
                    engine.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
            if (executor != null) {
                List<Runnable> neverRunTasks = executor.shutdownNow();
                assertThat(neverRunTasks).isEmpty();
                try {
                    while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                        // wait for completion ...
                    }
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
            if (engine != null && engine.isRunning()) {
                try {
                    while (!engine.await(5, TimeUnit.SECONDS)) {
                        // Wait for connector to stop completely ...
                    }
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
            if (callback != null) callback.accept(engine != null ? engine.isRunning() : false);
        } finally {
            engine = null;
            executor = null;
        }
    }

    /**
     * Get the maximum number of messages that can be obtained from the connector and held in-memory before they are
     * consumed by test methods using {@link #consumeRecord()}, {@link #consumeRecords(int)}, or
     * {@link #consumeRecords(int, Consumer)}.
     * 
     * <p>
     * By default this method return {@code 100}.
     * 
     * @return the maximum number of records that can be enqueued
     */
    protected int getMaximumEnqueuedRecordCount() {
        return 100;
    }

    /**
     * Start the connector using the supplied connector configuration, where upon completion the status of the connector is
     * logged.
     * 
     * @param connectorClass the connector class; may not be null
     * @param connectorConfig the configuration for the connector; may not be null
     */
    protected void start(Class<? extends SourceConnector> connectorClass, Configuration connectorConfig) {
        start(connectorClass, connectorConfig, (success, msg, error) -> {
            if (success) {
                logger.info(msg);
            } else {
                logger.error(msg, error);
            }
        });
    }

    /**
     * Start the connector using the supplied connector configuration.
     * 
     * @param connectorClass the connector class; may not be null
     * @param connectorConfig the configuration for the connector; may not be null
     * @param callback the function that will be called when the engine fails to start the connector or when the connector
     *            stops running after completing successfully or due to an error; may be null
     */
    protected void start(Class<? extends SourceConnector> connectorClass, Configuration connectorConfig, CompletionCallback callback) {
        Configuration config = Configuration.copy(connectorConfig)
                                            .with(EmbeddedEngine.ENGINE_NAME, "testing-connector")
                                            .with(EmbeddedEngine.CONNECTOR_CLASS, connectorClass.getName())
                                            .with(FileOffsetBackingStore.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH)
                                            .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                                            .build();
        latch = new CountDownLatch(1);
        CompletionCallback wrapperCallback = (success, msg, error) -> {
            try {
                if (callback != null) callback.handle(success, msg, error);
            } finally {
                latch.countDown();
            }
        };

        // Create the connector ...
        engine = EmbeddedEngine.create()
                               .using(config)
                               .notifying(consumedLines::add)
                               .using(this.getClass().getClassLoader())
                               .using(wrapperCallback)
                               .build();

        // Submit the connector for asynchronous execution ...
        assertThat(executor).isNull();
        executor = Executors.newFixedThreadPool(1);
        executor.execute(engine);
    }

    /**
     * Set the maximum amount of time that the {@link #consumeRecord()}, {@link #consumeRecords(int)}, and
     * {@link #consumeRecords(int, Consumer)} methods block while waiting for each record before returning <code>null</code>.
     * 
     * @param timeout the timeout; must be positive
     * @param unit the time unit; may not be null
     */
    protected void setConsumeTimeout(long timeout, TimeUnit unit) {
        if (timeout < 0) throw new IllegalArgumentException("The timeout may not be negative");
        pollTimeoutInMs = unit.toMillis(timeout);
    }

    /**
     * Consume a single record from the connector.
     * 
     * @return the next record that was returned from the connector, or null if no such record has been produced by the connector
     * @throws InterruptedException if the thread was interrupted while waiting for a record to be returned
     */
    protected SourceRecord consumeRecord() throws InterruptedException {
        return consumedLines.poll(pollTimeoutInMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Try to consume the specified number of records from the connector, and return the actual number of records that were
     * consumed. Use this method when your test does not care what the records might contain.
     * 
     * @param numberOfRecords the number of records that should be consumed
     * @return the actual number of records that were consumed
     * @throws InterruptedException if the thread was interrupted while waiting for a record to be returned
     */
    protected int consumeRecords(int numberOfRecords) throws InterruptedException {
        return consumeRecords(numberOfRecords, null);
    }

    /**
     * Try to consume the specified number of records from the connector, calling the given function for each, and return the
     * actual number of records that were consumed.
     * 
     * @param numberOfRecords the number of records that should be consumed
     * @param recordConsumer the function that should be called with each consumed record
     * @return the actual number of records that were consumed
     * @throws InterruptedException if the thread was interrupted while waiting for a record to be returned
     */
    protected int consumeRecords(int numberOfRecords, Consumer<SourceRecord> recordConsumer) throws InterruptedException {
        int recordsConsumed = 0;
        for (int i = 0; i != numberOfRecords; ++i) {
            SourceRecord record = consumedLines.poll(pollTimeoutInMs, TimeUnit.MILLISECONDS);
            if (record != null) {
                ++recordsConsumed;
                if (recordConsumer != null) {
                    recordConsumer.accept(record);
                }
            }
        }
        return recordsConsumed;
    }

    /**
     * Try to consume all of the messages that have already been returned by the connector.
     * 
     * @param recordConsumer the function that should be called with each consumed record
     * @return the number of records that were consumed
     */
    protected int consumeAvailableRecords(Consumer<SourceRecord> recordConsumer) {
        List<SourceRecord> records = new LinkedList<>();
        consumedLines.drainTo(records);
        if (recordConsumer != null) {
            records.forEach(recordConsumer);
        }
        return records.size();
    }

    /**
     * Wait for a maximum amount of time until the first record is available.
     * 
     * @param timeout the maximum amount of time to wait; must not be negative
     * @param unit the time unit for {@code timeout}
     * @return {@code true} if records are available, or {@code false} if the timeout occurred and no records are available
     */
    protected boolean waitForAvailableRecords(long timeout, TimeUnit unit) {
        assertThat(timeout).isGreaterThanOrEqualTo(0);
        long now = System.currentTimeMillis();
        long stop = now + unit.toMillis(timeout);
        while (System.currentTimeMillis() < stop) {
            if (!consumedLines.isEmpty()) break;
        }
        return consumedLines.isEmpty() ? false : true;
    }

    /**
     * Assert that the connector is currently running.
     */
    protected void assertConnectorIsRunning() {
        assertThat(engine.isRunning()).isTrue();
    }

    /**
     * Assert that the connector is NOT currently running.
     */
    protected void assertConnectorNotRunning() {
        assertThat(engine.isRunning()).isFalse();
    }

    /**
     * Assert that there are no records to consume.
     */
    protected void assertNoRecordsToConsume() {
        assertThat(consumedLines.isEmpty()).isTrue();
    }

    protected void assertKey(SourceRecord record, String pkField, int pk) {
        Struct key = (Struct) record.key();
        assertThat(key.get(pkField)).isEqualTo(pk);
    }

    protected void assertTombstone(SourceRecord record) {
        assertThat(record.key()).isNotNull();
        assertThat(record.keySchema()).isNotNull();
        assertThat(record.value()).isNull();
        assertThat(record.valueSchema()).isNull();
    }

    protected void assertTombstone(SourceRecord record, String pkField, int pk) {
        assertKey(record,pkField,pk);
        assertTombstone(record);
    }

    protected void assertInsert(SourceRecord record, String pkField, int pk) {
        assertKey(record,pkField,pk);
        assertThat(record.key()).isNotNull();
        assertThat(record.keySchema()).isNotNull();
        assertThat(record.value()).isNotNull();
        assertThat(record.valueSchema()).isNotNull();
    }

    protected void assertUpdate(SourceRecord record, String pkField, int pk) {
        assertInsert(record,pkField,pk);    // currently the same as an insert
    }

    protected void print(SourceRecord record) {
        StringBuilder sb = new StringBuilder("SourceRecord{");
        sb.append("sourcePartition=").append(record.sourcePartition());
        sb.append(", sourceOffset=").append(record.sourceOffset());
        sb.append(", topic=").append(record.topic());
        sb.append(", kafkaPartition=").append(record.kafkaPartition());
        sb.append(", key=");
        append(record.key(), sb);
        sb.append(", value=");
        append(record.value(), sb);
        sb.append("}");
        Testing.print(sb.toString());
    }

    protected void printJson(SourceRecord record) {
        JsonNode keyJson = null;
        JsonNode valueJson = null;
        try {
            // First serialize and deserialize the key ...
            byte[] keyBytes = keyJsonConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
            keyJson = keyJsonDeserializer.deserialize(record.topic(), keyBytes);
            // then the value ...
            byte[] valueBytes = valueJsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            valueJson = valueJsonDeserializer.deserialize(record.topic(), valueBytes);
            // And finally get ready to print it ...
            JsonNodeFactory nodeFactory = new JsonNodeFactory(false);
            ObjectNode message = nodeFactory.objectNode();
            message.set("key", keyJson);
            message.set("value", valueJson);
            Testing.print("Message on topic '" + record.topic() + "':");
            Testing.print(prettyJson(message));
        } catch (Throwable t) {
            Testing.printError(t);
            Testing.print("Problem with message on topic '" + record.topic() + "':");
            if ( keyJson != null ) {
                Testing.print("valid key = " + prettyJson(keyJson));
            } else {
                Testing.print("invalid key");
            }
            if ( valueJson != null ) {
                Testing.print("valid value = " + prettyJson(valueJson));
            } else {
                Testing.print("invalid value");
            }
            fail(t.getMessage());
        }
    }

    protected String prettyJson(JsonNode json) {
        try {
            return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(json);
        } catch (Throwable t) {
            Testing.printError(t);
            fail(t.getMessage());
            assert false : "Will not get here";
            return null;
        }
    }

    protected void append(Object obj, StringBuilder sb) {
        if (obj == null) {
            sb.append("null");
        } else if (obj instanceof Schema) {
            Schema schema = (Schema) obj;
            sb.append('{');
            sb.append("name=").append(schema.name());
            sb.append(", type=").append(schema.type());
            sb.append(", optional=").append(schema.isOptional());
            sb.append(", fields=");
            boolean first = true;
            for (Field field : schema.fields()) {
                if (first)
                    first = false;
                else
                    sb.append(", ");
                sb.append("name=").append(field.name());
                sb.append(", index=").append(field.index());
                sb.append(", schema=");
                append(field.schema(), sb);
            }
            sb.append('}');
        } else if (obj instanceof Struct) {
            Struct s = (Struct) obj;
            sb.append('{');
            boolean first = true;
            for (Field field : s.schema().fields()) {
                if (first)
                    first = false;
                else
                    sb.append(", ");
                sb.append(field.name()).append('=');
                append(s.get(field), sb);
            }
            sb.append('}');
        } else if (obj instanceof Map<?, ?>) {
            Map<?, ?> map = (Map<?, ?>) obj;
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (first)
                    first = false;
                else
                    sb.append(", ");
                append(entry.getKey(), sb);
                sb.append('=');
                append(entry.getValue(), sb);
            }
            sb.append('}');
        } else if (obj instanceof List<?>) {
            List<?> list = (List<?>) obj;
            sb.append('[');
            boolean first = true;
            for (Object value : list) {
                if (first)
                    first = false;
                else
                    sb.append(", ");
                append(value, sb);
            }
            sb.append(']');
        } else if (obj instanceof String) {
            sb.append('"').append(obj.toString()).append('"');
        } else {
            sb.append(obj.toString());
        }
    }
}
