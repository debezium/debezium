/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import static org.fest.assertions.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.file.FileStreamSourceConnector;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.Before;
import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.engine.DebeziumEngine;
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
        final DebeziumEngine<SourceRecord> engine = DebeziumEngine.create(Connect.class)
                .using(props)
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
        final DebeziumEngine<SourceRecord> engine = DebeziumEngine.create(Connect.class)
                .using(props)
                .notifying((records, committer) -> {
                    assertThat(records.size()).isGreaterThanOrEqualTo(NUMBER_OF_LINES - 1);
                    records.forEach(r -> assertThat(r.topic()).isEqualTo("trftopicX"));
                    Integer groupCount = records.size() / NUMBER_OF_LINES;

                    for (SourceRecord r : records) {
                        assertThat((String) r.value()).isNotEqualTo("Generated line number 1");
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
        final DebeziumEngine<SourceRecord> engine = DebeziumEngine.create(Connect.class)
                .using(props)
                .notifying((records, committer) -> {
                })
                .using(this.getClass().getClassLoader())
                .build();
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
