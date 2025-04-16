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
import java.util.Map;

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
    protected static final UniqueDatabase DATABASE = new UniqueDatabase("logical_server_name",
            "connector_noblob_mode_test") {
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
