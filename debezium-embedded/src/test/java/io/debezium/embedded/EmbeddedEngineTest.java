/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import static org.fest.assertions.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.file.FileStreamSourceConnector;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.SafeObjectInputStream;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.simple.SimpleSourceConnector;
import io.debezium.doc.FixFor;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.engine.format.Json;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.util.Collect;
import io.debezium.util.LoggingContext;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
public class EmbeddedEngineTest extends AbstractConnectorTest {

    private static final int NUMBER_OF_LINES = 10;

    private static final Path TEST_FILE_PATH = Testing.Files.createTestingPath("file-connector-input.txt").toAbsolutePath();
    private static final Charset UTF8 = StandardCharsets.UTF_8;

    private File inputFile;
    private int nextConsumedLineNumber;
    private int linesAdded;
    private Configuration connectorConfig;

    public static class FilterTransform implements Transformation<SourceRecord> {

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public SourceRecord apply(SourceRecord record) {
            return ((String) record.value()).equals("Generated line number 1") ? null : record;
        }

        @Override
        public ConfigDef config() {
            return null;
        }

        @Override
        public void close() {
        }
    }

    @Before
    public void beforeEach() throws Exception {
        nextConsumedLineNumber = 1;
        linesAdded = 0;
        Testing.Files.delete(TEST_FILE_PATH);
        inputFile = Testing.Files.createTestingFile(TEST_FILE_PATH);
        // Basic connector configuration; the remaining engine configuration props are set in base class in startup
        connectorConfig = Configuration.create()
                .with(FileStreamSourceConnector.FILE_CONFIG, TEST_FILE_PATH)
                .with(FileStreamSourceConnector.TOPIC_CONFIG, "topicX")
                .build();
    }

    @Test
    public void interruptedTaskShutsDown() throws Exception {

        Configuration config = Configuration.create()
                .with(EmbeddedEngine.ENGINE_NAME, "testing-connector")
                .with(EmbeddedEngine.CONNECTOR_CLASS, InterruptedConnector.class)
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH)
                .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                .with(EmbeddedEngine.OFFSET_STORAGE, InterruptingOffsetStore.class)
                .build();

        CountDownLatch firstLatch = new CountDownLatch(1);

        engine = EmbeddedEngine.create()
                .using(config)
                .notifying((records, committer) -> {
                })
                .using(this.getClass().getClassLoader())
                .using((success, message, error) -> {
                    if (error != null) {
                        logger.error("Error while shutting down", error);
                    }
                    firstLatch.countDown();
                })
                .build();

        ExecutorService exec = Executors.newFixedThreadPool(1);
        exec.execute(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        firstLatch.await(5000, TimeUnit.MILLISECONDS);
        assertThat(firstLatch.getCount()).isEqualTo(0);
    }

    @Test
    public void interruptedOffsetCommitShutsDown() throws Exception {

        Configuration config = Configuration.create()
                .with(SimpleSourceConnector.BATCH_COUNT, 1)
                .with(EmbeddedEngine.ENGINE_NAME, "testing-connector")
                .with(EmbeddedEngine.CONNECTOR_CLASS, SimpleSourceConnector.class)
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH)
                .with(EmbeddedEngine.OFFSET_STORAGE, InterruptingOffsetStore.class)
                .build();

        CountDownLatch firstLatch = new CountDownLatch(1);

        engine = EmbeddedEngine.create()
                .using(config)
                .using(OffsetCommitPolicy.always())
                .notifying((records, committer) -> {

                    for (SourceRecord record : records) {
                        committer.markProcessed(record);
                    }
                    committer.markBatchFinished();
                })
                .using(this.getClass().getClassLoader())
                .using((success, message, error) -> {
                    if (error != null) {
                        logger.error("Error while shutting down", error);
                    }
                    firstLatch.countDown();
                })
                .build();

        ExecutorService exec = Executors.newFixedThreadPool(1);
        exec.execute(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        firstLatch.await(5000, TimeUnit.MILLISECONDS);
        assertThat(firstLatch.getCount()).isEqualTo(0);
    }

    @Test
    public void shouldStartAndUseFileConnectorUsingMemoryOffsetStorage() throws Exception {
        // Add initial content to the file ...
        appendLinesToSource(NUMBER_OF_LINES);

        // Start the connector ...
        start(FileStreamSourceConnector.class, connectorConfig);

        // Verify the first 10 lines were found ...
        consumeLines(NUMBER_OF_LINES);
        assertNoRecordsToConsume();

        for (int i = 1; i != 5; ++i) {
            // Add a few more lines, and then verify they are consumed ...
            appendLinesToSource(NUMBER_OF_LINES);
            consumeLines(NUMBER_OF_LINES);
            assertNoRecordsToConsume();
        }

        // Stop the connector ..
        stopConnector();

        // Add several more lines ...
        appendLinesToSource(NUMBER_OF_LINES);
        assertNoRecordsToConsume();

        // Start the connector again ...
        start(FileStreamSourceConnector.class, connectorConfig);

        // Verify that we see the correct line number, meaning that offsets were recorded correctly ...
        consumeLines(NUMBER_OF_LINES);
        assertNoRecordsToConsume();
    }

    @Test
    @FixFor("DBZ-1080")
    public void shouldWorkToUseCustomChangeConsumer() throws Exception {
        // Add initial content to the file ...
        appendLinesToSource(NUMBER_OF_LINES);

        Configuration config = Configuration.copy(connectorConfig)
                .with(EmbeddedEngine.ENGINE_NAME, "testing-connector")
                .with(EmbeddedEngine.CONNECTOR_CLASS, FileStreamSourceConnector.class)
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH)
                .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                .build();

        CountDownLatch firstLatch = new CountDownLatch(1);
        CountDownLatch allLatch = new CountDownLatch(6);

        // create an engine with our custom class
        engine = EmbeddedEngine.create()
                .using(config)
                .notifying((records, committer) -> {
                    assertThat(records.size()).isGreaterThanOrEqualTo(NUMBER_OF_LINES);
                    Integer groupCount = records.size() / NUMBER_OF_LINES;

                    for (SourceRecord r : records) {
                        committer.markProcessed(r);
                    }

                    committer.markBatchFinished();
                    firstLatch.countDown();
                    for (int i = 0; i < groupCount; i++) {
                        allLatch.countDown();
                    }
                })
                .using(this.getClass().getClassLoader())
                .build();

        ExecutorService exec = Executors.newFixedThreadPool(1);
        exec.execute(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        firstLatch.await(5000, TimeUnit.MILLISECONDS);
        assertThat(firstLatch.getCount()).isEqualTo(0);

        for (int i = 0; i < 5; i++) {
            // Add a few more lines, and then verify they are consumed ...
            appendLinesToSource(NUMBER_OF_LINES);
            Thread.sleep(10);
        }
        allLatch.await(5000, TimeUnit.MILLISECONDS);
        assertThat(allLatch.getCount()).isEqualTo(0);

        // Stop the connector ...
        stopConnector();
    }

    @Test
    public void shouldRunDebeziumEngine() throws Exception {
        // Add initial content to the file ...
        appendLinesToSource(NUMBER_OF_LINES);

        final Properties props = new Properties();
        props.setProperty("name", "debezium-engine");
        props.setProperty("connector.class", "org.apache.kafka.connect.file.FileStreamSourceConnector");
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty("offset.flush.interval.ms", "0");
        props.setProperty("file", TEST_FILE_PATH.toAbsolutePath().toString());
        props.setProperty("topic", "topicX");

        CountDownLatch firstLatch = new CountDownLatch(1);
        CountDownLatch allLatch = new CountDownLatch(6);

        // create an engine with our custom class
        final DebeziumEngine<RecordChangeEvent<SourceRecord>> engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(props)
                .notifying((records, committer) -> {
                    assertThat(records.size()).isGreaterThanOrEqualTo(NUMBER_OF_LINES);
                    Integer groupCount = records.size() / NUMBER_OF_LINES;

                    for (RecordChangeEvent<SourceRecord> r : records) {
                        committer.markProcessed(r);
                    }

                    committer.markBatchFinished();
                    firstLatch.countDown();
                    for (int i = 0; i < groupCount; i++) {
                        allLatch.countDown();
                    }
                })
                .using(this.getClass().getClassLoader())
                .build();

        ExecutorService exec = Executors.newFixedThreadPool(1);
        exec.execute(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        firstLatch.await(5000, TimeUnit.MILLISECONDS);
        assertThat(firstLatch.getCount()).isEqualTo(0);

        for (int i = 0; i < 5; i++) {
            // Add a few more lines, and then verify they are consumed ...
            appendLinesToSource(NUMBER_OF_LINES);
            Thread.sleep(10);
        }
        allLatch.await(5000, TimeUnit.MILLISECONDS);
        assertThat(allLatch.getCount()).isEqualTo(0);

        // Stop the connector ...
        stopConnector();
    }

    @Test
    @FixFor("DBZ-2897")
    public void shouldRunEngineWithConsumerSettingOffsets() throws Exception {
        // Add initial content to the file ...
        appendLinesToSource(NUMBER_OF_LINES);

        String TEST_TOPIC = "topicX";
        String CUSTOM_SOURCE_OFFSET_PARTITION = "test_topic_partition1";
        Long EXPECTED_CUSTOM_OFFSET = 1L;

        final Properties props = new Properties();
        props.setProperty("name", "debezium-engine");
        props.setProperty("connector.class", "org.apache.kafka.connect.file.FileStreamSourceConnector");
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty("offset.flush.interval.ms", "0");
        props.setProperty("file", TEST_FILE_PATH.toAbsolutePath().toString());
        props.setProperty("topic", TEST_TOPIC);

        CountDownLatch firstLatch = new CountDownLatch(1);
        CountDownLatch allLatch = new CountDownLatch(6);

        // create an engine with our custom class
        final DebeziumEngine<RecordChangeEvent<SourceRecord>> engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(props)
                .notifying((records, committer) -> {
                    assertThat(records.size()).isGreaterThanOrEqualTo(NUMBER_OF_LINES);
                    Integer groupCount = records.size() / NUMBER_OF_LINES;

                    for (RecordChangeEvent<SourceRecord> r : records) {
                        DebeziumEngine.Offsets offsets = committer.buildOffsets();
                        offsets.set(CUSTOM_SOURCE_OFFSET_PARTITION, EXPECTED_CUSTOM_OFFSET);
                        logger.info(r.record().sourceOffset().toString());
                        committer.markProcessed(r, offsets);
                    }

                    committer.markBatchFinished();
                    firstLatch.countDown();
                    for (int i = 0; i < groupCount; i++) {
                        allLatch.countDown();
                    }
                })
                .using(this.getClass().getClassLoader())
                .build();

        ExecutorService exec = Executors.newFixedThreadPool(1);
        exec.execute(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        firstLatch.await(5000, TimeUnit.MILLISECONDS);
        assertThat(firstLatch.getCount()).isEqualTo(0);

        for (int i = 0; i < 5; i++) {
            // Add a few more lines, and then verify they are consumed ...
            appendLinesToSource(NUMBER_OF_LINES);
            Thread.sleep(10);
        }
        allLatch.await(5000, TimeUnit.MILLISECONDS);
        assertThat(allLatch.getCount()).isEqualTo(0);

        SafeObjectInputStream inputStream = new SafeObjectInputStream(java.nio.file.Files.newInputStream(OFFSET_STORE_PATH.toAbsolutePath()));
        Object obj = inputStream.readObject();
        Map<byte[], byte[]> raw = (Map) obj;
        Set<Map.Entry<byte[], byte[]>> fileOffsetStoreEntrySingleton = raw.entrySet();
        assertThat(fileOffsetStoreEntrySingleton.size()).isEqualTo(1);
        Map.Entry<byte[], byte[]> fileOffsetEntry = fileOffsetStoreEntrySingleton.iterator().next();
        ByteBuffer offsetJsonString = fileOffsetEntry.getValue() != null ? ByteBuffer.wrap(fileOffsetEntry.getValue()) : null;
        JsonDeserializer jsonDeserializer = new JsonDeserializer();
        JsonNode partitionToOffsetMap = jsonDeserializer.deserialize(TEST_TOPIC, offsetJsonString.array());
        Long actualOffset = partitionToOffsetMap.get(CUSTOM_SOURCE_OFFSET_PARTITION).asLong();
        assertThat(actualOffset).isEqualTo(EXPECTED_CUSTOM_OFFSET);

        // Stop the connector ...
        stopConnector();
    }

    @Test
    public void shouldExecuteSmt() throws Exception {
        // Add initial content to the file ...
        appendLinesToSource(NUMBER_OF_LINES);

        final Properties props = new Properties();
        props.setProperty("name", "debezium-engine");
        props.setProperty("connector.class", "org.apache.kafka.connect.file.FileStreamSourceConnector");
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty("offset.flush.interval.ms", "0");
        props.setProperty("file", TEST_FILE_PATH.toAbsolutePath().toString());
        props.setProperty("topic", "topicX");
        props.setProperty("transforms", "filter, router");
        props.setProperty("transforms.router.type", "org.apache.kafka.connect.transforms.RegexRouter");
        props.setProperty("transforms.router.regex", "(.*)");
        props.setProperty("transforms.router.replacement", "trf$1");
        props.setProperty("transforms.filter.type", "io.debezium.embedded.EmbeddedEngineTest$FilterTransform");

        CountDownLatch firstLatch = new CountDownLatch(1);
        CountDownLatch allLatch = new CountDownLatch(5);

        // create an engine with our custom class
        final DebeziumEngine<RecordChangeEvent<SourceRecord>> engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(props)
                .notifying((records, committer) -> {
                    assertThat(records.size()).isGreaterThanOrEqualTo(NUMBER_OF_LINES - 1);
                    records.forEach(r -> assertThat(r.record().topic()).isEqualTo("trftopicX"));
                    Integer groupCount = records.size() / NUMBER_OF_LINES;

                    for (RecordChangeEvent<SourceRecord> r : records) {
                        assertThat((String) r.record().value()).isNotEqualTo("Generated line number 1");
                        committer.markProcessed(r);
                    }

                    committer.markBatchFinished();
                    firstLatch.countDown();
                    for (int i = 0; i < groupCount; i++) {
                        allLatch.countDown();
                    }
                })
                .using(this.getClass().getClassLoader())
                .build();

        ExecutorService exec = Executors.newFixedThreadPool(1);
        exec.execute(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        firstLatch.await(5000, TimeUnit.MILLISECONDS);
        assertThat(firstLatch.getCount()).isEqualTo(0);

        for (int i = 0; i < 5; i++) {
            // Add a few more lines, and then verify they are consumed ...
            appendLinesToSource(NUMBER_OF_LINES);
            Thread.sleep(10);
        }
        allLatch.await(5000, TimeUnit.MILLISECONDS);
        assertThat(allLatch.getCount()).isEqualTo(0);

        // Stop the connector ...
        stopConnector();
    }

    @Test(expected = DebeziumException.class)
    public void invalidSmt() throws Exception {
        // Add initial content to the file ...
        appendLinesToSource(NUMBER_OF_LINES);

        final Properties props = new Properties();
        props.setProperty("name", "debezium-engine");
        props.setProperty("connector.class", "org.apache.kafka.connect.file.FileStreamSourceConnector");
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty("offset.flush.interval.ms", "0");
        props.setProperty("file", TEST_FILE_PATH.toAbsolutePath().toString());
        props.setProperty("topic", "topicX");
        props.setProperty("transforms", "router");
        props.setProperty("transforms.router.type", "org.apache.kafka.connect.transforms.Regex");
        props.setProperty("transforms.router.regex", "(.*)");
        props.setProperty("transforms.router.replacement", "trf$1");

        // create an engine with our custom class
        DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(props)
                .notifying((records, committer) -> {
                })
                .using(this.getClass().getClassLoader())
                .build();
    }

    @Test
    @FixFor("DBZ-1807")
    public void shouldRunDebeziumEngineWithJson() throws Exception {
        // Add initial content to the file ...
        appendLinesToSource(NUMBER_OF_LINES);

        final Properties props = new Properties();
        props.setProperty("name", "debezium-engine");
        props.setProperty("connector.class", "org.apache.kafka.connect.file.FileStreamSourceConnector");
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty("offset.flush.interval.ms", "0");
        props.setProperty("file", TEST_FILE_PATH.toAbsolutePath().toString());
        props.setProperty("topic", "topicX");
        props.setProperty("converter.schemas.enable", "false");

        CountDownLatch firstLatch = new CountDownLatch(1);
        CountDownLatch allLatch = new CountDownLatch(6);

        // create an engine with our custom class
        final DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying((records, committer) -> {
                    assertThat(records.size()).isGreaterThanOrEqualTo(NUMBER_OF_LINES);
                    Integer groupCount = records.size() / NUMBER_OF_LINES;

                    for (ChangeEvent<String, String> r : records) {
                        Assertions.assertThat(r.key()).isNull();
                        Assertions.assertThat(r.value()).startsWith("\"Generated line number ");
                        committer.markProcessed(r);
                    }

                    committer.markBatchFinished();
                    firstLatch.countDown();
                    for (int i = 0; i < groupCount; i++) {
                        allLatch.countDown();
                    }
                })
                .using(this.getClass().getClassLoader())
                .build();

        ExecutorService exec = Executors.newFixedThreadPool(1);
        exec.execute(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        firstLatch.await(5000, TimeUnit.MILLISECONDS);
        assertThat(firstLatch.getCount()).isEqualTo(0);

        for (int i = 0; i < 5; i++) {
            // Add a few more lines, and then verify they are consumed ...
            appendLinesToSource(NUMBER_OF_LINES);
            Thread.sleep(10);
        }
        allLatch.await(5000, TimeUnit.MILLISECONDS);
        assertThat(allLatch.getCount()).isEqualTo(0);

        // Stop the connector ...
        stopConnector();
    }

    protected void appendLinesToSource(int numberOfLines) throws IOException {
        CharSequence[] lines = new CharSequence[numberOfLines];
        for (int i = 0; i != numberOfLines; ++i) {
            lines[i] = generateLine(linesAdded + i + 1);
        }
        java.nio.file.Files.write(inputFile.toPath(), Collect.arrayListOf(lines), UTF8, StandardOpenOption.APPEND);
        linesAdded += numberOfLines;
    }

    protected String generateLine(int lineNumber) {
        return "Generated line number " + lineNumber;
    }

    protected void consumeLines(int numberOfLines) throws InterruptedException {
        consumeRecords(numberOfLines, 3, record -> {
            String line = record.value().toString();
            assertThat(line).isEqualTo(generateLine(nextConsumedLineNumber));
            ++nextConsumedLineNumber;
        },
                false);
    }
}

class InterruptedConnector extends SimpleSourceConnector {

    @Override
    public Class<? extends Task> taskClass() {
        return InterruptedTask.class;
    }
}

class InterruptedTask extends SimpleSourceConnector.SimpleConnectorTask {

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        throw new InterruptedException();
    }
}

class InterruptingOffsetStore implements OffsetBackingStore {

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> collection) {
        // called by the offset reader. return null for no offsets stored.
        return new CompletableFuture<Map<ByteBuffer, ByteBuffer>>() {
            @Override
            public Map<ByteBuffer, ByteBuffer> get(long timeout, TimeUnit unit) {
                return new HashMap<ByteBuffer, ByteBuffer>();
            }

            @Override
            public Map<ByteBuffer, ByteBuffer> get() {
                return new HashMap<ByteBuffer, ByteBuffer>();
            }
        };
    }

    /**
     * Implementation that throws InterruptedException when offset commits are called.
     */
    @Override
    public Future<Void> set(Map<ByteBuffer, ByteBuffer> map, Callback<Void> callback) {
        return new CompletableFuture<Void>() {
            @Override
            public Void get() throws InterruptedException {
                throw new InterruptedException();
            }

            @Override
            public Void get(long timeout, TimeUnit unit) throws InterruptedException {
                throw new InterruptedException();
            }
        };
    }

    @Override
    public void configure(WorkerConfig workerConfig) {
    }
}
