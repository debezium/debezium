/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.file.FileStreamSourceConnector;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.util.SafeObjectInputStream;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Charsets;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.simple.SimpleSourceConnector;
import io.debezium.doc.FixFor;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.Header;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.JsonByteArray;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.util.LoggingContext;
import io.debezium.util.Testing;
import io.debezium.util.Throwables;

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
            final String payload = (String) record.value();
            return payload.equals("Generated line number 1") || payload.equals("Generated line number 2") ? null
                    : record;
        }

        @Override
        public ConfigDef config() {
            return new ConfigDef();
        }

        @Override
        public void close() {
        }
    }

    public static class FilterPredicate implements Predicate<SourceRecord> {
        @Override
        public ConfigDef config() {
            return new ConfigDef();
        }

        @Override
        public boolean test(SourceRecord sourceRecord) {
            return sourceRecord.value().equals("Generated line number 1");
        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> map) {

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
    public void verifyNonAsciiContentHandledCorrectly() throws Exception {

        linesAdded += DebeziumEngineTestUtils.appendLinesToSource(inputFile, "Ñ ñ", NUMBER_OF_LINES, linesAdded);

        final Properties props = new Properties();
        props.setProperty("name", "debezium-engine");
        props.setProperty("connector.class", "org.apache.kafka.connect.file.FileStreamSourceConnector");
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty("offset.flush.interval.ms", "0");
        props.setProperty("file", TEST_FILE_PATH.toAbsolutePath().toString());
        props.setProperty("topic", "topicX");

        CountDownLatch firstLatch = new CountDownLatch(1);

        // create an engine with our custom class
        final DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class, Json.class, Json.class)
                .using(props)
                .notifying((records, committer) -> {
                    assertThat(records.size()).isGreaterThanOrEqualTo(NUMBER_OF_LINES);
                    for (ChangeEvent<String, String> record : records) {
                        assertThat(record.value()).contains("Ñ");
                    }

                    for (ChangeEvent<String, String> r : records) {
                        committer.markProcessed(r);
                    }

                    committer.markBatchFinished();
                    firstLatch.countDown();
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

        // Stop the connector ..
        stopConnector();

    }

    @Test
    public void interruptedTaskShutsDown() throws Exception {

        final Properties props = new Properties();
        props.put(EmbeddedEngineConfig.ENGINE_NAME.name(), "testing-connector");
        props.put(EmbeddedEngineConfig.CONNECTOR_CLASS.name(), InterruptedConnector.class.getName());
        props.put(EmbeddedEngineConfig.OFFSET_FLUSH_INTERVAL_MS.name(), 0);
        props.put(EmbeddedEngineConfig.OFFSET_STORAGE.name(), InterruptingOffsetStore.class.getName());
        props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.put(DebeziumEngine.OFFSET_FLUSH_INTERVAL_MS_PROP, "0");

        CountDownLatch firstLatch = new CountDownLatch(1);

        final DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(props)
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

        final Properties props = new Properties();
        props.put(EmbeddedEngineConfig.ENGINE_NAME.name(), "testing-connector");
        props.put(EmbeddedEngineConfig.CONNECTOR_CLASS.name(), SimpleSourceConnector.class.getName());
        props.put(EmbeddedEngineConfig.OFFSET_STORAGE.name(), InterruptingOffsetStore.class.getName());
        props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.put(SimpleSourceConnector.BATCH_COUNT, 1);
        props.put(DebeziumEngine.OFFSET_FLUSH_INTERVAL_MS_PROP, "0");

        CountDownLatch firstLatch = new CountDownLatch(1);

        final DebeziumEngine engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(props)
                .using(OffsetCommitPolicy.always())
                .notifying((records, committer) -> {

                    for (RecordChangeEvent<SourceRecord> record : records) {
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

        final Properties props = new Properties();
        props.put(EmbeddedEngineConfig.ENGINE_NAME.name(), "testing-connector");
        props.put(EmbeddedEngineConfig.CONNECTOR_CLASS.name(), FileStreamSourceConnector.class.getName());
        props.put(EmbeddedEngineConfig.OFFSET_FLUSH_INTERVAL_MS.name(), 0);
        props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.put(FileStreamSourceConnector.FILE_CONFIG, TEST_FILE_PATH.toAbsolutePath().toString());
        props.put(FileStreamSourceConnector.TOPIC_CONFIG, "topicX");
        props.put(DebeziumEngine.OFFSET_FLUSH_INTERVAL_MS_PROP, "0");

        CountDownLatch firstLatch = new CountDownLatch(1);
        CountDownLatch allLatch = new CountDownLatch(6);

        // create an engine with our custom class
        final DebeziumEngine engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
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
        props.setProperty("transforms", "header");
        props.setProperty("transforms.header.type", AddHeaderTransform.class.getName());

        CountDownLatch firstLatch = new CountDownLatch(1);
        CountDownLatch allLatch = new CountDownLatch(6);

        Headers expectedHeaders = new ConnectHeaders();
        expectedHeaders.addString("headerKey", "headerValue");

        // create an engine with our custom class
        final DebeziumEngine<RecordChangeEvent<SourceRecord>> engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(props)
                .notifying((records, committer) -> {
                    assertThat(records.size()).isGreaterThanOrEqualTo(NUMBER_OF_LINES);
                    Integer groupCount = records.size() / NUMBER_OF_LINES;

                    for (RecordChangeEvent<SourceRecord> r : records) {
                        assertThat(r.record().headers()).isEqualTo(expectedHeaders);
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
        props.setProperty("predicates", "filter");
        props.setProperty("predicates.filter.type", FilterPredicate.class.getName());
        props.setProperty("transforms", "filter, router");
        props.setProperty("transforms.router.type", "org.apache.kafka.connect.transforms.RegexRouter");
        props.setProperty("transforms.router.regex", "(.*)");
        props.setProperty("transforms.router.replacement", "trf$1");
        props.setProperty("transforms.filter.type", "io.debezium.embedded.EmbeddedEngineTest$FilterTransform");
        props.setProperty("transforms.filter.predicate", "filter");

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
        props.setProperty("transforms", "header");
        props.setProperty("transforms.header.type", AddHeaderTransform.class.getName());

        CountDownLatch firstLatch = new CountDownLatch(1);
        CountDownLatch allLatch = new CountDownLatch(6);

        EmbeddedEngineHeader<String> expectedHeader = new EmbeddedEngineHeader<>("headerKey", "\"headerValue\"");

        // create an engine with our custom class
        final DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class, Json.class, Json.class)
                .using(props)
                .notifying((records, committer) -> {
                    assertThat(records.size()).isGreaterThanOrEqualTo(NUMBER_OF_LINES);
                    int groupCount = records.size() / NUMBER_OF_LINES;

                    for (ChangeEvent<String, String> r : records) {
                        assertThat(r.key()).isNull();
                        assertThat(r.value()).startsWith("\"Generated line number ");

                        List<Header<String>> headers = r.headers();
                        assertThat(headers).allMatch(h -> h.getKey().equals(expectedHeader.getKey()) && h.getValue().equals(expectedHeader.getValue()));

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
    @FixFor("DBZ-5926")
    public void shouldRunDebeziumEngineWithMismatchedTypes() throws Exception {
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
        props.setProperty("transforms", "header");
        props.setProperty("transforms.header.type", AddHeaderTransform.class.getName());

        CountDownLatch firstLatch = new CountDownLatch(1);
        CountDownLatch allLatch = new CountDownLatch(6);

        EmbeddedEngineHeader<byte[]> expectedHeader = new EmbeddedEngineHeader<>("headerKey", "\"headerValue\"".getBytes(StandardCharsets.UTF_8));

        // create an engine with our custom class
        final DebeziumEngine<ChangeEvent<String, byte[]>> engine = DebeziumEngine.create(Json.class, JsonByteArray.class, JsonByteArray.class)
                .using(props)
                .notifying((records, committer) -> {
                    assertThat(records.size()).isGreaterThanOrEqualTo(NUMBER_OF_LINES);
                    int groupCount = records.size() / NUMBER_OF_LINES;

                    for (ChangeEvent<String, byte[]> r : records) {
                        assertThat(r.key()).isNull();
                        assertThat(new String(r.value(), Charsets.UTF_8)).startsWith("\"Generated line number ");

                        List<Header<byte[]>> headers = r.headers();
                        assertThat(headers).hasSize(1);
                        assertThat(headers).allMatch(h -> h.getKey().equals(expectedHeader.getKey()) && Arrays.equals(h.getValue(), expectedHeader.getValue()));

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
    @FixFor("DBZ-5583")
    public void verifyBadCommitPolicyClassName() {

        final Properties props = new Properties();
        props.put(EmbeddedEngineConfig.ENGINE_NAME.name(), "testing-connector");
        props.put(EmbeddedEngineConfig.CONNECTOR_CLASS.name(), SimpleSourceConnector.class.getName());
        props.put(EmbeddedEngineConfig.OFFSET_COMMIT_POLICY.name(), "badclassname"); // force ClassNotFoundException
        props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        // props.put(DebeziumEngine.OFFSET_FLUSH_INTERVAL_MS_PROP, "0");

        final AtomicBoolean exceptionCaught = new AtomicBoolean(false);

        final DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying((records, committer) -> {
                })
                .using(this.getClass().getClassLoader())
                .using((success, message, error) -> {
                    Throwable rootCause = Throwables.getRootCause(error);
                    assertThat(rootCause).isInstanceOf(ClassNotFoundException.class);
                    assertThat(rootCause.getMessage()).contains("badclassname");
                    exceptionCaught.set(true);
                })
                .build();

        engine.run();

        assertThat(exceptionCaught.get()).isTrue();
    }

    @FixFor("DBZ-4720")
    @Test
    public void validationThrowsException() throws Exception {
        // Add initial content to the file ...
        appendLinesToSource(NUMBER_OF_LINES);

        // Start the connector ...
        AtomicReference<String> errorReference = new AtomicReference<>();
        start(FileStreamSourceConnector.class, Configuration.from(new Properties()), (success, message, error) -> {
            if (message != null) {
                errorReference.set(message);
            }
        });

        assertNoRecordsToConsume();

        assertThat(errorReference.get()).isNotNull();
        assertThat(errorReference.get()).contains("Connector configuration is not valid. ");
        assertThat(isEngineRunning.get()).isFalse();
    }

    @Test
    @FixFor("DBZ-7099")
    public void shouldHandleNoDefaultOffsetFlushInterval() throws IOException, InterruptedException {
        final Properties props = new Properties();
        props.put(EmbeddedEngineConfig.ENGINE_NAME.name(), "testing-connector");
        props.put(EmbeddedEngineConfig.CONNECTOR_CLASS.name(), SimpleSourceConnector.class.getName());
        props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.put(EmbeddedEngineConfig.WAIT_FOR_COMPLETION_BEFORE_INTERRUPT_MS.name(), "10");

        final CountDownLatch engineRunning = new CountDownLatch(1);
        final CountDownLatch engineStopped = new CountDownLatch(1);
        final AtomicBoolean engineSucceeded = new AtomicBoolean(false);

        final DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying((records, committer) -> {
                    engineRunning.countDown();
                })
                .using(this.getClass().getClassLoader())
                .using(new DebeziumEngine.ConnectorCallback() {
                    @Override
                    public void connectorStarted() {
                        isEngineRunning.compareAndExchange(false, true);
                    }
                })
                .using((success, message, error) -> {
                    engineSucceeded.set(success);
                    engineStopped.countDown();
                })
                .build();

        ExecutorService exec = Executors.newFixedThreadPool(1);
        exec.execute(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        engineRunning.await(100, TimeUnit.MILLISECONDS);
        assertThat(isEngineRunning.get()).isTrue();

        engine.close();
        engineStopped.await(100, TimeUnit.MILLISECONDS);
        assertThat(engineSucceeded.get()).isTrue();
    }

    protected void appendLinesToSource(int numberOfLines) throws IOException {
        linesAdded += DebeziumEngineTestUtils.appendLinesToSource(inputFile, numberOfLines, linesAdded);
    }

    protected void consumeLines(int numberOfLines) throws InterruptedException {
        consumeRecords(numberOfLines, 3, record -> {
            String line = record.value().toString();
            assertThat(line).isEqualTo(DebeziumEngineTestUtils.generateLine(nextConsumedLineNumber));
            ++nextConsumedLineNumber;
        },
                false);
    }

    public static class AddHeaderTransform implements Transformation<SourceRecord> {

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public SourceRecord apply(SourceRecord record) {
            Headers headers = new ConnectHeaders();
            headers.addString("headerKey", "headerValue");

            record = record.newRecord(
                    record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp(), headers);

            return record;
        }

        @Override
        public ConfigDef config() {
            return new ConfigDef();
        }

        @Override
        public void close() {
        }
    }
}
