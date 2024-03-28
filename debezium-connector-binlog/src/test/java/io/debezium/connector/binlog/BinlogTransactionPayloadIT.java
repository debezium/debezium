/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.Envelope;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.SkipWhenDatabaseVersion;

@SkipWhenDatabaseVersion(check = LESS_THAN, major = 8, minor = 0, patch = 20, reason = "MySQL 8.0.20 started supporting binlog compression")
public abstract class BinlogTransactionPayloadIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final UUID PRODUCT_CODE = UUID.randomUUID();
    private static final String PRODUCT_NAME = "robot";
    private static final float PRODUCT_WEIGHT = 1.304f;

    private static final String PRODUCT_INSERT_STMT_1 = "INSERT INTO products (name, description, weight, code) VALUES ('" + PRODUCT_NAME + "', 'Toy robot', " +
            PRODUCT_WEIGHT + ", uuid_to_bin('" + PRODUCT_CODE + "'));";
    private static final String PRODUCT_INSERT_STMT_1_NO_UUID = "INSERT INTO products (name, description, weight) VALUES ('" + PRODUCT_NAME + "', 'Toy robot', " +
            PRODUCT_WEIGHT + ");";
    private static final String CUSTOMER_INSERT_STMT_1 = "INSERT INTO customers (first_name, last_name, email) VALUES ('Nitin', 'Agarwal', 'test1@abc.com' ); ";
    private static final String CUSTOMER_INSERT_STMT_2 = "INSERT INTO customers (first_name, last_name, email) VALUES ('Rajesh', 'Agarwal', 'test2@abc.com' ); ";
    private static final String ORDER_INSERT_STMT_1 = "INSERT INTO orders (order_date, purchaser, quantity, product_id) VALUES ('2016-01-16', 1001, 1, 1); ";

    private static final String CUSTOMER_UPDATE_STMT_1 = "UPDATE customers set first_name = 'Nitin1' where id = 1001; ";
    private static final String CUSTOMER_DELETE_STMT_1 = "DELETE from customers where id = 1001; ";

    private static final String ORDER_UPDATE_STMT_1 = "UPDATE orders set order_date = '2017-01-16' where order_number = 10001; ";
    private static final String ORDER_DELETE_STMT_1 = "DELETE from orders where order_number = 10001; ";

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-tp.txt").toAbsolutePath();

    private static final String SERVER_NAME = "transactionpayload_it";
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase(SERVER_NAME, "transactionpayload_test").withDbHistoryPath(SCHEMA_HISTORY_PATH);

    @Rule
    public SkipTestRule skipTest = new SkipTestRule();
    private Configuration config;

    @Before
    public void beforeEach() throws TimeoutException, IOException, SQLException, InterruptedException {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() throws SQLException {
        try {
            stopConnector();
            // MariaDB's binlog compression is set globally, so we need to toggle this off after the test.
            try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
                db.setBinlogCompressionOff();
            }
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    public void shouldCaptureMultipleWriteEvents() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NEVER)
                .build();

        start(getConnectorClass(), config);

        Debug.enable();
        assertConnectorIsRunning();

        int numCreateDatabase = 1;
        int numCreateTables = 3;

        SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables);
        assertThat(records).isNotNull();
        records.forEach(this::validate);

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                // We exclude the code column because the uuid_to_bin function isn't something that is
                // in MariaDB as UUID is a basic database type now.
                String sql = db.isMariaDb() ? PRODUCT_INSERT_STMT_1_NO_UUID : PRODUCT_INSERT_STMT_1;
                db.setBinlogCompressionOn();
                connection.execute(CUSTOMER_INSERT_STMT_1, CUSTOMER_INSERT_STMT_2, sql,
                        ORDER_INSERT_STMT_1, CUSTOMER_UPDATE_STMT_1, ORDER_UPDATE_STMT_1, ORDER_DELETE_STMT_1,
                        CUSTOMER_DELETE_STMT_1);
            }
        }
        SourceRecords dmlRecords = consumeRecordsByTopic(10);
        List<SourceRecord> customerDmls = dmlRecords.recordsForTopic(DATABASE.topicForTable("customers"));
        List<SourceRecord> productDmls = dmlRecords.recordsForTopic(DATABASE.topicForTable("products"));
        List<SourceRecord> orderDmls = dmlRecords.recordsForTopic(DATABASE.topicForTable("orders"));

        assertThat(customerDmls).hasSize(5);
        assertThat(productDmls).hasSize(1);
        Struct product = ((Struct) productDmls.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(product.get("id")).isInstanceOf(Integer.class);
        assertThat(product.get("name")).isEqualTo(PRODUCT_NAME);
        assertThat(product.get("weight")).isEqualTo(PRODUCT_WEIGHT);
        if (!isMariaDb()) {
            assertThat(((ByteBuffer) product.get("code")).array()).isEqualTo(uuidToByteArray(PRODUCT_CODE));
        }
        assertThat(orderDmls).hasSize(4);
    }

    private byte[] uuidToByteArray(UUID uuid) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return buffer.array();
    }
}
