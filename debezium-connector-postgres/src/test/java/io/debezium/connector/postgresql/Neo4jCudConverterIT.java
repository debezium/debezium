/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import org.junit.jupiter.api.BeforeEach;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.transforms.neo4j.AbstractNeo4jCudConverterIT;

public class Neo4jCudConverterIT extends AbstractNeo4jCudConverterIT<PostgresConnector> {

    @BeforeEach
    void prepareDatabase() {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
    }

    @Override
    protected Class<PostgresConnector> getConnectorClass() {
        return PostgresConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return TestHelper.create();
    }

    @Override
    protected Configuration.Builder getConfigurationBuilder() {
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, true);
    }

    @Override
    protected void createTables() throws Exception {
        TestHelper.execute(
                "DROP TABLE IF EXISTS order_items CASCADE;"
                        + "DROP TABLE IF EXISTS orders CASCADE;"
                        + "DROP TABLE IF EXISTS products CASCADE;"
                        + "DROP TABLE IF EXISTS customers CASCADE;"
                        + "CREATE TABLE customers ("
                        + "  id INTEGER PRIMARY KEY,"
                        + "  first_name VARCHAR(100),"
                        + "  last_name VARCHAR(100),"
                        + "  email VARCHAR(255)"
                        + ");"
                        + "CREATE TABLE products ("
                        + "  id INTEGER PRIMARY KEY,"
                        + "  name VARCHAR(200),"
                        + "  price DECIMAL(10,2)"
                        + ");"
                        + "CREATE TABLE orders ("
                        + "  id INTEGER PRIMARY KEY,"
                        + "  customer_id INTEGER REFERENCES customers(id),"
                        + "  total DECIMAL(10,2),"
                        + "  status VARCHAR(50)"
                        + ");"
                        + "CREATE TABLE order_items ("
                        + "  order_id INTEGER REFERENCES orders(id),"
                        + "  product_id INTEGER REFERENCES products(id),"
                        + "  quantity INTEGER,"
                        + "  PRIMARY KEY (order_id, product_id)"
                        + ")");
    }

    @Override
    protected void waitForStreamingStarted() throws InterruptedException {
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
    }

    @Override
    protected String topicName(String table) {
        return TestHelper.topicName("public." + table);
    }
}
