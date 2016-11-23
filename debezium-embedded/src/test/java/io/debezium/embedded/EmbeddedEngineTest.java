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

import org.apache.kafka.connect.file.FileStreamSourceConnector;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.config.Configuration;
import io.debezium.util.Collect;
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
        consumeRecords(numberOfLines, record -> {
            String line = record.value().toString();
            assertThat(line).isEqualTo(generateLine(nextConsumedLineNumber));
            ++nextConsumedLineNumber;
        });
    }
}
