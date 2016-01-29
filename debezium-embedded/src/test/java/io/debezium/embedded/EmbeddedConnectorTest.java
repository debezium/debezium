/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.file.FileStreamSourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.config.Configuration;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
public class EmbeddedConnectorTest implements Testing {

    private static final int NUMBER_OF_LINES = 10;

    private static final Path TEST_FILE_PATH = Testing.Files.createTestingPath("file-connector-input.txt");
    private static final Charset UTF8 = StandardCharsets.UTF_8;

    private ExecutorService executor;
    private EmbeddedConnector connector;
    private File inputFile;
    private BlockingQueue<SourceRecord> consumedLines;
    private int nextConsumedLineNumber;
    private int linesAdded;
    private OffsetBackingStore offsetStore;

    @Before
    public void beforeEach() throws Exception {
        nextConsumedLineNumber = 1;
        linesAdded = 0;
        consumedLines = new ArrayBlockingQueue<>(100);
        Testing.Files.delete(TEST_FILE_PATH);
        inputFile = Testing.Files.createTestingFile(TEST_FILE_PATH);
        executor = Executors.newFixedThreadPool(1);
    }

    @After
    public void afterEach() {
        stopConnector();
    }

    @Test
    public void shouldStartAndUseFileConnectorUsingMemoryOffsetStorage() throws Exception {
        // Set up the offset store ...
        offsetStore = new MemoryOffsetBackingStore();
        
        // Add initial content to the file ...
        appendLinesToSource(NUMBER_OF_LINES);

        // Create the connector ...
        connector = EmbeddedConnector.create()
                                     .using(Configuration.create()
                                                         .with(EmbeddedConnector.CONNECTOR_NAME, "file-connector")
                                                         .with(EmbeddedConnector.CONNECTOR_CLASS, FileStreamSourceConnector.class.getName())
                                                         .with(FileStreamSourceConnector.FILE_CONFIG, inputFile.getAbsolutePath())
                                                         .with(FileStreamSourceConnector.TOPIC_CONFIG, "topicX")
                                                         .build())
                                     .using(offsetStore)
                                     .using(OffsetCommitPolicy.always())
                                     .notifying(consumedLines::add)
                                     .build();

        // Submit the connector for asynchronous execution ...
        executor.execute(connector);

        // Verify the first 10 lines were found ...
        consumeOutput(NUMBER_OF_LINES);
        assertNothingToConsume();

        for (int i = 1; i != 5; ++i) {
            // Add a few more lines, and then verify they are consumed ...
            appendLinesToSource(NUMBER_OF_LINES);
            consumeOutput(NUMBER_OF_LINES);
            assertNothingToConsume();
        }

        // Stop the connector ...
        assertThat(connector.stop()).isTrue();

        // Add several more lines ...
        appendLinesToSource(NUMBER_OF_LINES);
        assertNothingToConsume();

        // Start the connector again ...
        executor.execute(connector);

        // Verify that we see the correct line number, meaning that offsets were recorded correctly ...
        consumeOutput(NUMBER_OF_LINES);
        assertNothingToConsume();
    }

    protected void appendLinesToSource(int numberOfLines) throws IOException {
        CharSequence[] lines = new CharSequence[numberOfLines];
        for (int i = 0; i != numberOfLines; ++i) {
            lines[i] = generateLine(linesAdded + i + 1);
        }
        java.nio.file.Files.write(inputFile.toPath(), Collect.arrayListOf(lines), UTF8, StandardOpenOption.APPEND);
        linesAdded += numberOfLines;
    }

    protected void appendLinesToSource(CharSequence... lines) throws IOException {
    }

    protected String generateLine(int lineNumber) {
        return "Generated line number " + lineNumber;
    }

    protected void consumeOutput(int numberOfLines) throws InterruptedException {
        for (int i = 0; i != numberOfLines; ++i) {
            SourceRecord record = consumedLines.poll(5, TimeUnit.SECONDS);
            String line = record.value().toString();
            assertThat(line).isEqualTo(generateLine(nextConsumedLineNumber));
            ++nextConsumedLineNumber;
        }
    }

    protected void assertNothingToConsume() {
        assertThat(consumedLines.isEmpty()).isTrue();
    }
    
    protected void stopConnector() {
        try {
            // Try to stop the connector ...
            if ( connector != null ) {
                connector.stop();
                try {
                    connector.await(5, TimeUnit.SECONDS);
                } catch ( InterruptedException e ) {
                    Thread.interrupted();
                }
            }
            List<Runnable> neverRunTasks = executor.shutdownNow();
            assertThat(neverRunTasks).isEmpty();
            try {
                executor.awaitTermination(10, TimeUnit.SECONDS); // wait for completion ...
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
            assertStopped();
        } finally {
            connector = null;
            executor = null;
        }
    }
    
    protected void assertStopped() {
        if (connector != null) {
            assertThat(connector.isRunning()).isFalse();
        }
    }
}
