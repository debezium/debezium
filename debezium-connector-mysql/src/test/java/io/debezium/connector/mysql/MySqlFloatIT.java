/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

/**
 * Verify correct range of FLOAT.
 *
 * @author Harvey Yue
 */
public class MySqlFloatIT extends AbstractAsyncEngineConnectorTest {
    private static final String TABLE_NAME = "DBZ3865";
    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-float.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("floatit", "float_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-3865")
    public void shouldHandleFloatAsFloatAndDouble() throws SQLException, InterruptedException {
        String includeTables = String.join(",",
                Collect.arrayListOf(DATABASE.qualifiedTableName(TABLE_NAME), DATABASE.qualifiedTableName("DBZ3865_2")));
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, includeTables)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        assertFloatChangeRecord(consumeInsert());

        // Clone the table `DBZ3865_2` and insert data
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            try (JdbcConnection conn = db.connect()) {
                String createDdl = "CREATE TABLE `DBZ3865_2` (\n"
                        + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
                        + "  `f1` FLOAT DEFAULT 5.6,\n"
                        + "  `f2` FLOAT(10, 2) DEFAULT NULL,\n"
                        + "  `f3` FLOAT(35, 5) DEFAULT NULL,\n"
                        + "  `f4_23` FLOAT(23) DEFAULT NULL,\n"
                        + "  `f4_24` FLOAT(24) DEFAULT NULL,\n"
                        + "  `f4_25` FLOAT(25) DEFAULT NULL,\n"
                        + "  `weight` FLOAT UNSIGNED DEFAULT '0',\n"
                        + "  PRIMARY KEY (`ID`)\n"
                        + ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;";
                String insertDml = "INSERT INTO DBZ3865_2(f1, f2, f3, f4_23, f4_24, f4_25, weight) VALUE (5.6, 5.61, 30.123456, 64.1, 64.1, 64.1, 64.1234);";
                conn.execute(createDdl, insertDml);
            }
        }

        SourceRecords records = consumeRecordsByTopic(2);
        assertThat(records).isNotNull();
        List<SourceRecord> events = records.recordsForTopic(DATABASE.topicForTable("DBZ3865_2"));
        assertFloatChangeRecord(events.get(0));

        stopConnector();
    }

    private SourceRecord consumeInsert() throws InterruptedException {
        final int numDatabase = 2;
        final int numTables = 4;
        final int numOthers = 1;

        SourceRecords records = consumeRecordsByTopic(numDatabase + numTables + numOthers);

        assertThat(records).isNotNull();

        List<SourceRecord> events = records.recordsForTopic(DATABASE.topicForTable(TABLE_NAME));
        assertThat(events).hasSize(1);

        return events.get(0);
    }

    private void assertFloatChangeRecord(SourceRecord sourceRecord) {
        assertThat(sourceRecord).isNotNull();
        final Struct change = ((Struct) sourceRecord.value()).getStruct("after");
        final float f2 = (float) 5.61;
        final float f3 = (float) 30.12346;

        assertThat(change.getFloat32("f1")).isEqualTo((float) 5.6);
        assertThat(change.getFloat64("f2")).isEqualTo(Double.valueOf(((Number) f2).doubleValue()));
        assertThat(change.getFloat64("f3")).isEqualTo(Double.valueOf(((Number) f3).doubleValue()));
        assertThat(change.getFloat32("f4_23")).isEqualTo((float) 64.1);
        assertThat(change.getFloat32("f4_24")).isEqualTo((float) 64.1);
        // Mysql will convert float(25) to double type
        assertThat(change.getFloat64("f4_25")).isEqualTo(64.1);
        // Mysql will treat "float unsigned" as float type
        assertThat(change.getFloat32("weight")).isEqualTo((float) 64.1234);
    }
}
