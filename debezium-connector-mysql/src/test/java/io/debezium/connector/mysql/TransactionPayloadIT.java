/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.util.Testing;

@SkipWhenDatabaseVersion(check = LESS_THAN, major = 8, minor = 0, patch = 20, reason = "MySQL 8.0.20 started supporting binlog compression")
public class TransactionPayloadIT extends AbstractConnectorTest {

    private static final String PRODUCT_INSERT_STMT_1 = "INSERT INTO products (name, description, weight) VALUES ('robot', 'Toy robot', 1.304);";
    private static final String CUSTOMER_INSERT_STMT_1 = "INSERT INTO customers (first_name, last_name, email) VALUES ('Nitin', 'Agarwal', 'test1@abc.com' ); ";
    private static final String CUSTOMER_INSERT_STMT_2 = "INSERT INTO customers (first_name, last_name, email) VALUES ('Rajesh', 'Agarwal', 'test2@abc.com' ); ";
    private static final String ORDER_INSERT_STMT_1 = "INSERT INTO orders (order_date, purchaser, quantity, product_id) VALUES ('2016-01-16', 1001, 1, 1); ";

    private static final String CUSTOMER_UPDATE_STMT_1 = "UPDATE customers set first_name = 'Nitin1' where id = 1001; ";
    private static final String CUSTOMER_DELETE_STMT_1 = "DELETE from customers where id = 1001; ";

    private static final String ORDER_UPDATE_STMT_1 = "UPDATE orders set order_date = '2017-01-16' where order_number = 10001; ";
    private static final String ORDER_DELETE_STMT_1 = "DELETE from orders where order_number = 10001; ";

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-tp.txt").toAbsolutePath();

    private static final String SERVER_NAME = "transactionpayload_it";
    private final UniqueDatabase DATABASE = new UniqueDatabase(SERVER_NAME, "transactionpayload_test").withDbHistoryPath(SCHEMA_HISTORY_PATH);
    @Rule
    public SkipTestRule skipTest = new SkipTestRule();
    private Configuration config;

    @Before
    public void beforeEach() throws TimeoutException, IOException, SQLException, InterruptedException {
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
    public void shouldCaptureMultipleWriteEvents() throws Exception {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .build();

        start(MySqlConnector.class, config);

        Testing.Debug.enable();
        assertConnectorIsRunning();

        int numCreateDatabase = 1;
        int numCreateTables = 3;

        SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables);
        assertThat(records).isNotNull();
        records.forEach(this::validate);

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("set binlog_transaction_compression=ON;");
                connection.execute(CUSTOMER_INSERT_STMT_1, CUSTOMER_INSERT_STMT_2, PRODUCT_INSERT_STMT_1,
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
        assertThat(orderDmls).hasSize(4);
    }
}
