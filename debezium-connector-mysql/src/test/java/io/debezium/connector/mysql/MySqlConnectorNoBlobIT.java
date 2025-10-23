/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.KeyValueStore;
import io.debezium.data.SchemaChangeHistory;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.JdbcConnection;

/**
 * Testing binlog_row_image=noblob mode in mysql
 *
 * @author Bue-Von-Hun
 */
public class MySqlConnectorNoBlobIT extends AbstractAsyncEngineConnectorTest {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-snapshot.txt").toAbsolutePath();
    protected static final UniqueDatabase DATABASE = new UniqueDatabase(
            "logical_server_name",
            "connector_noblob_mode_test") {
        @Override
        public ZoneId getTimezone() {
            return TimeZone.getDefault().toZoneId();
        }

        @Override
        protected JdbcConnection forTestDatabase(String databaseName, Map<String, Object> urlProperties) {
            return MySqlTestConnection.forTestDatabase(databaseName, urlProperties);
        }
    }
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);
    protected Configuration config;
    private KeyValueStore store;
    private SchemaChangeHistory schemaChanges;

    @Before
    public void beforeEach() {
        Files.delete(SCHEMA_HISTORY_PATH);
        DATABASE.createAndInitialize();

        store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + '.');
        schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());
    }

    @After
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
        final List<Struct> productRecrods = new ArrayList<>();

        // Getting the after image.
        products.forEach(val -> productRecrods.add(((Struct) val.value()).getStruct("after")));

        // Check that the image does not contain a text/blob column.
        productRecrods.forEach(val -> {
            final Schema schema = val.schema();
            final List<Field> fields = schema.fields();
            assertThat(fields).hasSize(2);
            fields.forEach(field -> assertThat(field.name()).isNotEqualTo("description"));
        });
    }

    /**
     * Test that both before and after sections exclude BLOB/TEXT columns in UPDATE operations
     * when binlog_row_image=NOBLOB mode is enabled.
     */
    @Test
    public void textAndBlobColumnShouldNotBeContainedInBeforeAndAfterSectionsDuringUpdate() throws InterruptedException, SQLException {
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

        // Check that before images do not contain BLOB/TEXT columns
        beforeImages.forEach(beforeImage -> {
            final Schema schema = beforeImage.schema();
            final List<Field> fields = schema.fields();
            assertThat(fields).hasSize(2); // Should only have 'id' and 'name' fields
            fields.forEach(field -> {
                assertThat(field.name()).isNotEqualTo("description");
                assertThat(field.name()).isIn("id", "name");
            });
        });

        // Check that after images do not contain BLOB/TEXT columns
        afterImages.forEach(afterImage -> {
            final Schema schema = afterImage.schema();
            final List<Field> fields = schema.fields();
            assertThat(fields).hasSize(2); // Should only have 'id' and 'name' fields
            fields.forEach(field -> {
                assertThat(field.name()).isNotEqualTo("description");
                assertThat(field.name()).isIn("id", "name");
            });
        });

        // Verify that the updates were captured correctly (name field should be updated)
        afterImages.forEach(afterImage -> {
            final String name = afterImage.getString("name");
            assertThat(name).startsWith("updated-");
        });
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

    private static String productsTableName() throws SQLException {
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            return db.isTableIdCaseSensitive() ? "products" : "Products";
        }
    }
}
