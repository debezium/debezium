/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.fest.assertions.Delta;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.data.SchemaUtil;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.EmbeddedEngine.CompletionCallback;
import io.debezium.embedded.EmbeddedEngine.ConnectorCallback;
import io.debezium.embedded.EmbeddedEngine.EmbeddedConfig;
import io.debezium.function.BooleanConsumer;
import io.debezium.junit.SkipTestRule;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.util.LoggingContext;
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
    
    @Rule
    public TestRule skipTestRule = new SkipTestRule();
    
    protected static final Path OFFSET_STORE_PATH = Testing.Files.createTestingPath("file-connector-offsets.txt").toAbsolutePath();

    private ExecutorService executor;
    protected EmbeddedEngine engine;
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
        LoggingContext.forConnector(getClass().getSimpleName(), "", "test");
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
        OFFSET_STORE_PATH.getParent().toFile().mkdirs();
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
            logger.info("Stopping the connector");
            // Try to stop the connector ...
            if (engine != null && engine.isRunning()) {
                engine.stop();
                try {
                    engine.await(8, TimeUnit.SECONDS);
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
     * Create a {@link CompletionCallback} that logs when the engine fails to start the connector or when the connector
     * stops running after completing successfully or due to an error
     * 
     * @return the logging {@link CompletionCallback}
     */
    protected CompletionCallback loggingCompletion() {
        return (success, msg, error) -> {
            if (success) {
                logger.info(msg);
            } else {
                logger.error(msg, error);
            }
        };
    }

    /**
     * Start the connector using the supplied connector configuration, where upon completion the status of the connector is
     * logged.
     * 
     * @param connectorClass the connector class; may not be null
     * @param connectorConfig the configuration for the connector; may not be null
     */
    protected void start(Class<? extends SourceConnector> connectorClass, Configuration connectorConfig) {
        start(connectorClass, connectorConfig, loggingCompletion(), null);
    }

    /**
     * Start the connector using the supplied connector configuration, where upon completion the status of the connector is
     * logged. The connector will stop immediately when the supplied predicate returns true.
     * 
     * @param connectorClass the connector class; may not be null
     * @param connectorConfig the configuration for the connector; may not be null
     * @param isStopRecord the function that will be called to determine if the connector should be stopped before processing
     *            this record; may be null if not needed
     */
    protected void start(Class<? extends SourceConnector> connectorClass, Configuration connectorConfig,
                         Predicate<SourceRecord> isStopRecord) {
        start(connectorClass, connectorConfig, loggingCompletion(), isStopRecord);
    }

    /**
     * Start the connector using the supplied connector configuration.
     * 
     * @param connectorClass the connector class; may not be null
     * @param connectorConfig the configuration for the connector; may not be null
     * @param callback the function that will be called when the engine fails to start the connector or when the connector
     *            stops running after completing successfully or due to an error; may be null
     */
    protected void start(Class<? extends SourceConnector> connectorClass, Configuration connectorConfig,
                         CompletionCallback callback) {
        start(connectorClass, connectorConfig, callback, null);
    }

    /**
     * Start the connector using the supplied connector configuration.
     * 
     * @param connectorClass the connector class; may not be null
     * @param connectorConfig the configuration for the connector; may not be null
     * @param isStopRecord the function that will be called to determine if the connector should be stopped before processing
     *            this record; may be null if not needed
     * @param callback the function that will be called when the engine fails to start the connector or when the connector
     *            stops running after completing successfully or due to an error; may be null
     */
    protected void start(Class<? extends SourceConnector> connectorClass, Configuration connectorConfig,
                         CompletionCallback callback, Predicate<SourceRecord> isStopRecord) {
        Configuration config = Configuration.copy(connectorConfig)
                                            .with(EmbeddedEngine.ENGINE_NAME, "testing-connector")
                                            .with(EmbeddedEngine.CONNECTOR_CLASS, connectorClass.getName())
                                            .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH)
                                            .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                                            .build();
        latch = new CountDownLatch(1);
        CompletionCallback wrapperCallback = (success, msg, error) -> {
            try {
                if (callback != null) callback.handle(success, msg, error);
            } finally {
                if (!success) {
                    // we only unblock if there was an error; in all other cases we're unblocking when a task has been started
                    latch.countDown();
                }
            }
            Testing.debug("Stopped connector");
        };
        
        ConnectorCallback connectorCallback = new ConnectorCallback() {
            @Override
            public void taskStarted() {
                // if this is called, it means a task has been started successfully so we can continue
                latch.countDown();
            }
        }; 
        
        // Create the connector ...
        engine = EmbeddedEngine.create()
                               .using(config)
                               .notifying((record) -> {
                                   if (isStopRecord != null && isStopRecord.test(record)) {
                                       logger.error("Stopping connector after record as requested");
                                       throw new ConnectException("Stopping connector after record as requested");
                                   }
                                   try {
                                       consumedLines.put(record);
                                   } catch (InterruptedException e) {
                                       Thread.interrupted();
                                   }
                               })
                               .using(this.getClass().getClassLoader())
                               .using(wrapperCallback)
                               .using(connectorCallback)
                               .build();

        // Submit the connector for asynchronous execution ...
        assertThat(executor).isNull();
        executor = Executors.newFixedThreadPool(1);
        executor.execute(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                // maybe it takes more time to start up, so just log a warning and continue
                logger.warn("The connector did not finish starting its task(s) or complete in the expected amount of time");
            }
        } catch (InterruptedException e) {
            if (Thread.interrupted()) {
                fail("Interrupted while waiting for engine startup");
            }
        }
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
        while (recordsConsumed < numberOfRecords) {
            SourceRecord record = consumedLines.poll(pollTimeoutInMs, TimeUnit.MILLISECONDS);
            if (record != null) {
                ++recordsConsumed;
                if (recordConsumer != null) {
                    recordConsumer.accept(record);
                }
                if (Testing.Debug.isEnabled()) {
                    Testing.debug("Consumed record " + recordsConsumed + " / " + numberOfRecords + " ("
                            + (numberOfRecords - recordsConsumed) + " more)");
                    debug(record);
                } else if (Testing.Print.isEnabled()) {
                    Testing.print("Consumed record " + recordsConsumed + " / " + numberOfRecords + " ("
                            + (numberOfRecords - recordsConsumed) + " more)");
                    print(record);
                }
            } else {
                // TODO Add support for cases when records are returned in more than one batch
                return recordsConsumed;
            }
        }
        return recordsConsumed;
    }

    /**
     * Try to consume and capture exactly the specified number of records from the connector.
     * 
     * @param numRecords the number of records that should be consumed
     * @return the collector into which the records were captured; never null
     * @throws InterruptedException if the thread was interrupted while waiting for a record to be returned
     */
    protected SourceRecords consumeRecordsByTopic(int numRecords) throws InterruptedException {
        SourceRecords records = new SourceRecords();
        consumeRecords(numRecords, records::add);
        return records;
    }

    protected class SourceRecords {
        private final List<SourceRecord> records = new ArrayList<>();
        private final Map<String, List<SourceRecord>> recordsByTopic = new HashMap<>();
        private final Map<String, List<SourceRecord>> ddlRecordsByDbName = new HashMap<>();

        public void add(SourceRecord record) {
            records.add(record);
            recordsByTopic.computeIfAbsent(record.topic(), (topicName) -> new ArrayList<SourceRecord>()).add(record);
            String dbName = getAffectedDatabase(record);
            if (dbName != null) ddlRecordsByDbName.computeIfAbsent(dbName, key -> new ArrayList<>()).add(record);
        }

        protected String getAffectedDatabase(SourceRecord record) {
            Struct value = (Struct) record.value();
            if (value != null) {
                Field dbField = value.schema().field(HistoryRecord.Fields.DATABASE_NAME);
                if (dbField != null) {
                    return value.getString(dbField.name());
                }
            }
            return null;
        }

        /**
         * Get the DDL events for the named database.
         * 
         * @param dbName the name of the database; may not be null
         * @return the DDL-related events; never null but possibly empty
         */
        public List<SourceRecord> ddlRecordsForDatabase(String dbName) {
            return ddlRecordsByDbName.get(dbName);
        }

        /**
         * Get the names of the databases that were affected by the DDL statements.
         * 
         * @return the set of database names; never null but possibly empty
         */
        public Set<String> databaseNames() {
            return ddlRecordsByDbName.keySet();
        }

        /**
         * Get the records on the given topic.
         * 
         * @param topicName the name of the topic.
         * @return the records for the topic; possibly null if there were no records produced on the topic
         */
        public List<SourceRecord> recordsForTopic(String topicName) {
            return recordsByTopic.get(topicName);
        }

        /**
         * Get the set of topics for which records were received.
         * 
         * @return the names of the topics; never null
         */
        public Set<String> topics() {
            return recordsByTopic.keySet();
        }

        public void forEachInTopic(String topic, Consumer<SourceRecord> consumer) {
            recordsForTopic(topic).forEach(consumer);
        }

        public void forEach(Consumer<SourceRecord> consumer) {
            records.forEach(consumer);
        }

        public List<SourceRecord> allRecordsInOrder() {
            return Collections.unmodifiableList(records);
        }

        public void print() {
            Testing.print("" + topics().size() + " topics: " + topics());
            recordsByTopic.forEach((k, v) -> {
                Testing.print(" - topic:'" + k + "'; # of events = " + v.size());
            });
            Testing.print("Records:");
            records.forEach(record -> AbstractConnectorTest.this.print(record));
        }
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
        VerifyRecord.hasValidKey(record, pkField, pk);
    }

    protected void assertInsert(SourceRecord record, String pkField, int pk) {
        VerifyRecord.isValidInsert(record, pkField, pk);
    }

    protected void assertUpdate(SourceRecord record, String pkField, int pk) {
        VerifyRecord.isValidUpdate(record, pkField, pk);
    }

    protected void assertDelete(SourceRecord record, String pkField, int pk) {
        VerifyRecord.isValidDelete(record, pkField, pk);
    }

    protected void assertTombstone(SourceRecord record, String pkField, int pk) {
        VerifyRecord.isValidTombstone(record, pkField, pk);
    }

    protected void assertTombstone(SourceRecord record) {
        VerifyRecord.isValidTombstone(record);
    }
    
    protected void assertOffset(SourceRecord record, Map<String,?> expectedOffset) {
        Map<String,?> offset = record.sourceOffset();
        assertThat(offset).isEqualTo(expectedOffset);
    }
    
    protected void assertOffset(SourceRecord record, String offsetField, Object expectedValue) {
        Map<String,?> offset = record.sourceOffset();
        Object value = offset.get(offsetField);
        assertSameValue(value,expectedValue);
    }
    
    protected void assertValueField(SourceRecord record, String fieldPath, Object expectedValue) {
        Object value = record.value();
        String[] fieldNames = fieldPath.split("/");
        String pathSoFar = null;
        for (int i=0; i!=fieldNames.length; ++i) {
            String fieldName = fieldNames[i];
            if (value instanceof Struct) {
                value = ((Struct)value).get(fieldName);
            } else {
                // We expected the value to be a struct ...
                String path = pathSoFar == null ? "record value" : ("'" + pathSoFar + "'");
                String msg = "Expected the " + path + " to be a Struct but was " + value.getClass().getSimpleName() + " in record: " + SchemaUtil.asString(record);
                fail(msg);
            }
            pathSoFar = pathSoFar == null ? fieldName : pathSoFar + "/" + fieldName;
        }
        assertSameValue(value,expectedValue);
    }
    
    private void assertSameValue(Object actual, Object expected) {
        if(expected instanceof Double || expected instanceof Float || expected instanceof BigDecimal) {
            // Value should be within 1%
            double expectedNumericValue = ((Number)expected).doubleValue();
            double actualNumericValue = ((Number)actual).doubleValue();
            assertThat(actualNumericValue).isEqualTo(expectedNumericValue, Delta.delta(0.01d*expectedNumericValue));
        } else if (expected instanceof Integer || expected instanceof Long || expected instanceof Short) {
            long expectedNumericValue = ((Number)expected).longValue();
            long actualNumericValue = ((Number)actual).longValue();
            assertThat(actualNumericValue).isEqualTo(expectedNumericValue);
        } else if (expected instanceof Boolean) {
            boolean expectedValue = ((Boolean)expected).booleanValue();
            boolean actualValue = ((Boolean)actual).booleanValue();
            assertThat(actualValue).isEqualTo(expectedValue);
        } else {
            assertThat(actual).isEqualTo(expected);
        }
    }

    /**
     * Assert that the supplied {@link Struct} is {@link Struct#validate() valid} and its {@link Struct#schema() schema}
     * matches that of the supplied {@code schema}.
     * 
     * @param value the value with a schema; may not be null
     */
    protected void assertSchemaMatchesStruct(SchemaAndValue value) {
        VerifyRecord.schemaMatchesStruct(value);
    }

    /**
     * Assert that the supplied {@link Struct} is {@link Struct#validate() valid} and its {@link Struct#schema() schema}
     * matches that of the supplied {@code schema}.
     * 
     * @param struct the {@link Struct} to validate; may not be null
     * @param schema the expected schema of the {@link Struct}; may not be null
     */
    protected void assertSchemaMatchesStruct(Struct struct, Schema schema) {
        VerifyRecord.schemaMatchesStruct(struct, schema);
    }

    /**
     * Assert that there was no exception in engine that would cause its termination.
     */
    protected void assertEngineIsRunning() {
        assertThat(engine.isRunning()).as("Engine should not fail due to an exception").isTrue();
    }

    /**
     * Validate that a {@link SourceRecord}'s key and value can each be converted to a byte[] and then back to an equivalent
     * {@link SourceRecord}.
     * 
     * @param record the record to validate; may not be null
     */
    protected void validate(SourceRecord record) {
        VerifyRecord.isValid(record);
    }

    protected void print(SourceRecord record) {
        VerifyRecord.print(record);
    }

    protected void debug(SourceRecord record) {
        VerifyRecord.debug(record);
    }

    protected void assertConfigurationErrors(Config config, io.debezium.config.Field field, int numErrors) {
        ConfigValue value = configValue(config, field.name());
        assertThat(value.errorMessages().size()).isEqualTo(numErrors);
    }

    protected void assertConfigurationErrors(Config config, io.debezium.config.Field field, int minErrorsInclusive,
                                             int maxErrorsInclusive) {
        ConfigValue value = configValue(config, field.name());
        assertThat(value.errorMessages().size()).isGreaterThanOrEqualTo(minErrorsInclusive);
        assertThat(value.errorMessages().size()).isLessThanOrEqualTo(maxErrorsInclusive);
    }

    protected void assertConfigurationErrors(Config config, io.debezium.config.Field field) {
        ConfigValue value = configValue(config, field.name());
        assertThat(value.errorMessages().size()).isGreaterThan(0);
    }

    protected void assertNoConfigurationErrors(Config config, io.debezium.config.Field... fields) {
        for (io.debezium.config.Field field : fields) {
            ConfigValue value = configValue(config, field.name());
            if (value != null) {
                if (!value.errorMessages().isEmpty()) {
                    fail("Error messages on field '" + field.name() + "': " + value.errorMessages());
                }
            }
        }
    }

    protected ConfigValue configValue(Config config, String fieldName) {
        return config.configValues().stream().filter(value -> value.name().equals(fieldName)).findFirst().orElse(null);
    }

    /**
     * Utility to read the last committed offset for the specified partition.
     * 
     * @param config the configuration of the engine used to persist the offsets
     * @param partition the partition
     * @return the map of partitions to offsets; never null but possibly empty
     */
    protected <T> Map<String, Object> readLastCommittedOffset(Configuration config, Map<String, T> partition) {
        return readLastCommittedOffsets(config, Arrays.asList(partition)).get(partition);
    }

    /**
     * Utility to read the last committed offsets for the specified partitions.
     * 
     * @param config the configuration of the engine used to persist the offsets
     * @param partitions the partitions
     * @return the map of partitions to offsets; never null but possibly empty
     */
    protected <T> Map<Map<String, T>, Map<String, Object>> readLastCommittedOffsets(Configuration config,
                                                                                    Collection<Map<String, T>> partitions) {
        config = config.edit().with(EmbeddedEngine.ENGINE_NAME, "testing-connector")
                       .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH)
                       .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                       .build();

        final String engineName = config.getString(EmbeddedEngine.ENGINE_NAME);
        Converter keyConverter = config.getInstance(EmbeddedEngine.INTERNAL_KEY_CONVERTER_CLASS, Converter.class);
        keyConverter.configure(config.subset(EmbeddedEngine.INTERNAL_KEY_CONVERTER_CLASS.name() + ".", true).asMap(), true);
        Converter valueConverter = config.getInstance(EmbeddedEngine.INTERNAL_VALUE_CONVERTER_CLASS, Converter.class);
        Configuration valueConverterConfig = config;
        if (valueConverter instanceof JsonConverter) {
            // Make sure that the JSON converter is configured to NOT enable schemas ...
            valueConverterConfig = config.edit().with(EmbeddedEngine.INTERNAL_VALUE_CONVERTER_CLASS + ".schemas.enable", false).build();
        }
        valueConverter.configure(valueConverterConfig.subset(EmbeddedEngine.INTERNAL_VALUE_CONVERTER_CLASS.name() + ".", true).asMap(),
                                 false);

        // Create the worker config, adding extra fields that are required for validation of a worker config
        // but that are not used within the embedded engine (since the source records are never serialized) ...
        Map<String, String> embeddedConfig = config.asMap(EmbeddedEngine.ALL_FIELDS);
        embeddedConfig.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        embeddedConfig.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        WorkerConfig workerConfig = new EmbeddedConfig(embeddedConfig);

        FileOffsetBackingStore offsetStore = new FileOffsetBackingStore();
        offsetStore.configure(workerConfig);
        offsetStore.start();
        try {
            OffsetStorageReaderImpl offsetReader = new OffsetStorageReaderImpl(offsetStore, engineName, keyConverter, valueConverter);
            return offsetReader.offsets(partitions);
        } finally {
            offsetStore.stop();
        }
    }
}
