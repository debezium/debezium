/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.KeyValueStore;
import io.debezium.data.SchemaChangeHistory;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing.Files;

/**
 * Testing binlog_row_image=noblob mode in mysql
 *
 * @author Bue-Von-Hun
 */
public class MySqlConnectorNoBlobIT extends AbstractAsyncEngineConnectorTest {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-noblob.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("logical_server_name", "connector_noblob_mode_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);
    protected Configuration config;
    private KeyValueStore store;
    private SchemaChangeHistory schemaChanges;

    @BeforeEach
    public void beforeEach() {
        Files.delete(SCHEMA_HISTORY_PATH);
        DATABASE.createAndInitialize();

        store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + '.');
        schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());
    }

    @AfterEach
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    protected Configuration.Builder simpleConfig() {
        return DATABASE.defaultConfig();
    }

    /**
     * Since we exclude text/blob columns during the snapshot process, we test that this behavior is done correctly.
     * */
    @Test
    public void textAndBlobColumnShouldNotBeContainedDuringSnapshot() throws InterruptedException, SQLException {
        // Use the DB configuration to define the connector's configuration.
        config = simpleConfig()
                .build();

        // Start the connector.
        start(MySqlConnector.class, config);

        // Poll for records.
        final int expected = 15;
        final int consumed = consumeAtLeast(expected);

        assertThat(consumed).isGreaterThanOrEqualTo(expected);
        final KeyValueStore.Collection products = store.collection(DATABASE.getDatabaseName(), productsTableName());
        final List<Struct> productRecords = new ArrayList<>();

        // Getting the after image.
        products.forEach(val -> productRecords.add(((Struct) val.value()).getStruct("after")));

        // Check that the image does not contain a text/blob column.
        productRecords.forEach(val -> {
            final Schema schema = val.schema();
            final List<Field> fields = schema.fields();
            assertThat(fields).hasSize(3);
        });

        products.forEach(val -> {
            final Struct struct = (Struct) val.value();
            final Struct after = struct.getStruct("after");
            final byte[] bytes = after.getBytes("description");
            final String descriptions = new String(bytes);
            assertThat(descriptions).isEqualTo("__debezium_unavailable_value");
        });
    }

    /**
     * Test that both before and after sections carry the unavailable-value placeholder for
     * BLOB/TEXT columns in UPDATE operations when binlog_row_image=NOBLOB mode is enabled.
     */
    @Test
    public void textAndBlobColumnShouldBeUnavailableValuePlaceholderInBeforeAndAfterSectionsDuringUpdate() throws InterruptedException, SQLException {
        // Use the DB configuration to define the connector's configuration.
        config = simpleConfig()
                .build();

        // Start the connector.
        start(MySqlConnector.class, config);

        // Wait for snapshot to complete
        final int snapshotExpected = 15;
        consumeAtLeast(snapshotExpected);

        // Clear the store to focus on UPDATE operations
        store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + '.');

        // Perform UPDATE operations that would normally trigger before/after sections
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            db.execute("UPDATE Products SET name = 'updated-scooter' WHERE id = 1");
            db.execute("UPDATE Products SET name = 'updated-battery' WHERE id = 2");
            db.execute("UPDATE Products SET name = 'updated-drill-bits' WHERE id = 3");
        }

        // Poll for UPDATE records
        final int updateExpected = 3;
        final int consumed = consumeAtLeast(updateExpected);

        assertThat(consumed).isGreaterThanOrEqualTo(updateExpected);
        final KeyValueStore.Collection products = store.collection(DATABASE.getDatabaseName(), productsTableName());

        final List<Struct> beforeImages = new ArrayList<>();
        final List<Struct> afterImages = new ArrayList<>();

        // Extract both before and after images from UPDATE records
        products.forEach(val -> {
            final Struct value = (Struct) val.value();
            final Struct beforeImage = value.getStruct("before");
            final Struct afterImage = value.getStruct("after");

            if (beforeImage != null) {
                beforeImages.add(beforeImage);
            }
            if (afterImage != null) {
                afterImages.add(afterImage);
            }
        });

        // Verify that we have the expected number of before/after images
        assertThat(beforeImages).hasSize(updateExpected);
        assertThat(afterImages).hasSize(updateExpected);

        // The value schema is fixed to the full table width, so the BLOB/TEXT column is always
        // present; unchanged BLOB/TEXT columns are omitted from the NOBLOB row image and must
        // surface as the unavailable-value placeholder instead.
        beforeImages.forEach(beforeImage -> {
            assertThat(beforeImage.schema().fields()).hasSize(3);
            assertThat(new String(beforeImage.getBytes("description"))).isEqualTo("__debezium_unavailable_value");
        });

        afterImages.forEach(afterImage -> {
            assertThat(afterImage.schema().fields()).hasSize(3);
            assertThat(new String(afterImage.getBytes("description"))).isEqualTo("__debezium_unavailable_value");
        });

        // Verify that the updates were captured correctly (name field should be updated)
        afterImages.forEach(afterImage -> {
            final String name = afterImage.getString("name");
            assertThat(name).startsWith("updated-");
        });
    }

    /**
     * Test that the before section carries the unavailable-value placeholder for BLOB/TEXT
     * columns in DELETE operations when binlog_row_image=NOBLOB mode is enabled.
     */
    @Test
    public void textAndBlobColumnShouldBeUnavailableValuePlaceholderInBeforeSectionDuringDelete() throws InterruptedException, SQLException {
        // Use the DB configuration to define the connector's configuration.
        config = simpleConfig()
                .build();

        // Start the connector.
        start(MySqlConnector.class, config);

        // Wait for snapshot to complete
        final int snapshotExpected = 15;
        consumeAtLeast(snapshotExpected);

        // Delete a row so that the binlog contains a delete event with a NOBLOB before image
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            db.execute("DELETE FROM Products WHERE id = 1");
        }

        final SourceRecords records = consumeRecordsByTopic(1);
        final List<SourceRecord> deleteRecords = records.recordsForTopic(DATABASE.topicForTable(productsTableName()));
        assertThat(deleteRecords).hasSize(1);

        final Struct value = (Struct) deleteRecords.get(0).value();
        final Struct beforeImage = value.getStruct("before");
        assertThat(beforeImage).isNotNull();
        assertThat(beforeImage.schema().fields()).hasSize(3);
        assertThat(beforeImage.getInt32("id")).isEqualTo(1);
        assertThat(beforeImage.getString("name")).isEqualTo("scooter");
        assertThat(new String(beforeImage.getBytes("description"))).isEqualTo("__debezium_unavailable_value");
    }

    private int consumeAtLeast(int minNumber) throws InterruptedException {
        final SourceRecords records = consumeRecordsByTopic(minNumber);
        final int count = records.allRecordsInOrder().size();
        records.forEach(record -> {
            VerifyRecord.isValid(record);
            store.add(record);
            schemaChanges.add(record);
        });
        return count;
    }

    private String productsTableName() throws SQLException {
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            return db.isTableIdCaseSensitive() ? "products" : "Products";
        }
    }
}
