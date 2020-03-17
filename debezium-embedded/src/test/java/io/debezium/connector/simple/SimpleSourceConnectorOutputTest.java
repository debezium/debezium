/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.simple;

import static org.fest.assertions.Assertions.assertThat;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.junit.Test;

import io.debezium.document.Array;
import io.debezium.document.ArrayReader;
import io.debezium.document.ArrayWriter;
import io.debezium.document.Document;
import io.debezium.embedded.ConnectorOutputTest;
import io.debezium.util.IoUtil;
import io.debezium.util.Testing;

/**
 * A test case for the {@link SimpleSourceConnector} that is also able to test and verify the behavior of
 * {@link ConnectorOutputTest}.
 *
 * @author Randall Hauch
 */
public class SimpleSourceConnectorOutputTest extends ConnectorOutputTest {

    protected static final String TOPIC_NAME = "some-topic";

    /**
     * Run the connector with no known expected results so that it generates the results.
     *
     * @throws Exception if there is an error
     */
    @Test
    public void shouldGenerateExpected() throws Exception {
        int numBatches = 1;
        int numRecordsPerBatch = 10;
        // Testing.Debug.enable();

        Path dir = Testing.Files.createTestingPath("simple/gen-expected").toAbsolutePath();
        Testing.Files.delete(dir);

        // Create the configuration file in this directory ...
        Properties config = new Properties();
        config.put(ConnectorConfig.NAME_CONFIG, "simple-connector-1");
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SimpleSourceConnector.class.getName());
        config.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        config.put(SimpleSourceConnector.BATCH_COUNT, Integer.toString(numBatches));
        config.put(SimpleSourceConnector.RECORD_COUNT_PER_BATCH, Integer.toString(numRecordsPerBatch));
        config.put(SimpleSourceConnector.TOPIC_NAME, TOPIC_NAME);
        writeConfigurationFileWithDefaultName(dir, config);

        Properties env = new Properties();
        env.put(ConnectorOutputTest.ENV_CONNECTOR_TIMEOUT_IN_SECONDS, "1");
        writeEnvironmentFileWithDefaultName(dir, env);

        Path expectedResults = dir.resolve(DEFAULT_EXPECTED_RECORDS_FILENAME);
        assertThat(Files.exists(expectedResults)).isFalse();

        // Run the connector to generate the results ...
        runConnector("gen-expected", dir);

        // Check that the expected records now exist ...
        assertExpectedRecords(expectedResults, numBatches, numRecordsPerBatch);

        // Append a stop command to the expected results ...
        appendStop(expectedResults);

        // Run the connector again (with fresh offsets) to read the expected results ...
        cleanOffsetStorage();
        runConnector("gen-expected", dir);
    }

    /**
     * Run the connector with connector configuration and expected results files, which are read in one step.
     */
    @Test
    public void shouldRunConnectorFromFilesInOneStep() {
        runConnector("simple-test-a", "src/test/resources/simple/test/a");
    }

    /**
     * Run the connector with connector configuration and expected results files, which are read in two steps.
     */
    @Test
    public void shouldRunConnectorFromFilesInTwoSteps() {
        runConnector("simple-test-b", "src/test/resources/simple/test/b");
    }

    /**
     * Run the connector with connector configuration and expected results files, but find a mismatch in the results.
     */
    @Test(expected = AssertionError.class)
    public void shouldRunConnectorFromFilesAndFindMismatch() {
        // Testing.Debug.enable();
        Testing.Print.disable();
        runConnector("simple-test-c", "src/test/resources/simple/test/c");
    }

    /**
     * Run the connector with connector configuration and expected results files, which are read in one step.
     * The connector includes timestamps that vary with each run, and the expected results have no no timestamps.
     * However, this test filters out the timestamps from the matching logic.
     */
    @Test
    public void shouldRunConnectorFromFilesInOneStepWithTimestamps() {
        // Testing.Debug.enable();
        runConnector("simple-test-d", "src/test/resources/simple/test/d");
    }

    /**
     * Run the connector and verify that {@link org.apache.kafka.connect.errors.RetriableException} is handled.
     */
    @Test
    public void shouldRecoverFromRetriableException() {
        // Testing.Debug.enable();
        runConnector("simple-test-e", "src/test/resources/simple/test/e");
    }

    protected void writeConfigurationFileWithDefaultName(Path dir, Properties props) throws IOException {
        Path configFilePath = dir.resolve(DEFAULT_CONNECTOR_PROPERTIES_FILENAME);
        writeConfigurationFile(configFilePath, props);
    }

    protected void writeEnvironmentFileWithDefaultName(Path dir, Properties props) throws IOException {
        Path configFilePath = dir.resolve(DEFAULT_ENV_PROPERTIES_FILENAME);
        writeConfigurationFile(configFilePath, props);
    }

    protected void writeConfigurationFile(Path configFilePath, Properties props) throws IOException {
        File configFile = Testing.Files.createTestingFile(configFilePath);
        try (OutputStream ostream = new FileOutputStream(configFile)) {
            props.store(ostream, "MockConnector configuration");
        }
    }

    protected Properties readConfiguration(Path configFilePath) throws IOException {
        File configFile = Testing.Files.createTestingFile(configFilePath);
        Properties props = new Properties();
        try (InputStream ostream = new FileInputStream(configFile)) {
            props.load(ostream);
        }
        return props;
    }

    protected void appendStop(Path results) throws IOException {
        appendCommand(results, Document.create(CONTROL_KEY, CONTROL_STOP));
    }

    protected Array readResults(File file) throws IOException {
        return ArrayReader.defaultReader().readArray(file);
    }

    protected void appendCommand(Path results, Document command) throws IOException {
        assertThat(command).isNotNull();
        assertThat(Files.exists(results)).isTrue();
        Array arrayOfDocuments = readResults(results.toFile());
        arrayOfDocuments.add(command);
        try (OutputStream stream = new FileOutputStream(results.toFile())) {
            ArrayWriter.prettyWriter().write(arrayOfDocuments, stream);
        }
        if (Testing.Debug.isEnabled()) {
            String content = IoUtil.read(results.toFile());
            Testing.debug("expected results file '" + results + "' after appending command:");
            Testing.debug(content);
        }
    }

    protected void assertExpectedRecords(Path path, int batchCount, int recordsPerBatch) throws IOException {
        assertThat(Files.exists(path)).isTrue();
        if (Testing.Debug.isEnabled()) {
            String content = IoUtil.read(path.toFile());
            Testing.debug("expected results file '" + path + "':");
            Testing.debug(content);
        }
        Array expected = readResults(path.toFile());
        int expectedId = 0;
        int expectedBatch = 1;
        int expectedRecord = 0;
        Iterator<Array.Entry> docs = expected.iterator();
        while (docs.hasNext()) {
            Document doc = docs.next().getValue().asDocument();
            if (doc.has(CONTROL_KEY)) {
                // This is a command so skip ...
                continue;
            }
            ++expectedId;
            ++expectedRecord;
            if (expectedRecord > recordsPerBatch) {
                ++expectedBatch;
                expectedRecord = 1;
            }

            Document sourcePartition = doc.getDocument("sourcePartition");
            assertThat(sourcePartition.getString("source")).isEqualTo("simple");
            Document offset = doc.getDocument("sourceOffset");
            assertThat(offset.getInteger("id")).isEqualTo(expectedId);
            assertThat(doc.getString("topic")).isEqualTo(TOPIC_NAME);
            assertThat(doc.getInteger("kafkaPartition")).isEqualTo(1);

            Document key = doc.getDocument("key");
            assertThat(key.getInteger("id")).isEqualTo(expectedId);

            Document value = doc.getDocument("value");
            assertThat(value.getInteger("batch")).isEqualTo(expectedBatch);
            assertThat(value.getInteger("record")).isEqualTo(expectedRecord);

            Document keySchema = doc.getDocument("keySchema");
            assertThat(keySchema.getString("name")).isEqualTo("simple.key");
            assertThat(keySchema.getString("type")).isEqualToIgnoringCase(Schema.Type.STRUCT.name());
            assertThat(keySchema.getBoolean("optional")).isEqualTo(false);
            Array keySchemaFields = keySchema.getArray("fields");
            Document keyIdField = keySchemaFields.get(0).asDocument();
            assertRequiredFieldSchema(keyIdField, "id", Schema.Type.INT32);

            Document valueSchema = doc.getDocument("valueSchema");
            assertThat(valueSchema.getString("name")).isEqualTo("simple.value");
            assertThat(valueSchema.getString("type")).isEqualToIgnoringCase(Schema.Type.STRUCT.name());
            assertThat(valueSchema.getBoolean("optional")).isEqualTo(false);
            Array valueSchemaFields = valueSchema.getArray("fields");
            Document batchField = valueSchemaFields.get(0).asDocument();
            assertRequiredFieldSchema(batchField, "batch", Schema.Type.INT32);
            Document recordField = valueSchemaFields.get(1).asDocument();
            assertRequiredFieldSchema(recordField, "record", Schema.Type.INT32);
        }
        assertThat(expectedBatch).isEqualTo(batchCount);
        assertThat(expectedId).isEqualTo(batchCount * recordsPerBatch);
    }

    protected void assertFieldSchema(Document doc, String fieldName, Schema.Type type, boolean optional) {
        assertThat(doc.getString("field")).isEqualTo(fieldName);
        assertThat(doc.getString("type")).isEqualToIgnoringCase(type.name());
        assertThat(doc.getBoolean("optional")).isEqualTo(optional);
    }

    protected void assertRequiredFieldSchema(Document doc, String fieldName, Schema.Type type) {
        assertFieldSchema(doc, fieldName, type, false);
    }

    protected void assertOptionalFieldSchema(Document doc, String fieldName, Schema.Type type) {
        assertFieldSchema(doc, fieldName, type, true);
    }
}
