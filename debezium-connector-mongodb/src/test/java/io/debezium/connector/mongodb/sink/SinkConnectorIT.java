/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoDatabase;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.Module;
import io.debezium.connector.mongodb.MongoDbSinkConnector;
import io.debezium.connector.mongodb.TestHelper;
import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.MongoDbDeployment;
import io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper;
import io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper.DATABASE;

public interface SinkConnectorIT {

    MongoDbDeployment getMongoDbDeployment();

    Logger LOGGER = LoggerFactory.getLogger(SinkConnectorIT.class);
    String DATABASE_NAME = "inventory";
    String SOURCE_CONNECTOR_NAME = "inventory";
    String SINK_CONNECTOR_NAME = "mongodb-sink";

    default Configuration.Builder mySqlSourceConfig() {
        return Configuration.create()
                .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
                .with("database.hostname", "mysql")
                .with("database.port", "3306")
                .with("database.user", "debezium")
                .with("database.password", "dbz")
                .with("database.server.id", "148045")
                .with("topic.prefix", "dbserver1")
                .with("database.include.list", "inventory")
                .with("schema.history.internal.kafka.bootstrap.servers", TestInfrastructureHelper.KAFKA_HOSTNAME + ":9092")
                .with("schema.history.internal.kafka.topic", "schema-changes.inventory");
    }

    default void sendSourceData() {
        TestInfrastructureHelper.defaultDebeziumContainer(Module.version());
        TestInfrastructureHelper.startContainers(DATABASE.MYSQL);
        final Configuration config = mySqlSourceConfig().build();
        TestInfrastructureHelper.getDebeziumContainer().registerConnector(SOURCE_CONNECTOR_NAME, ConnectorConfiguration.from(config.asMap()));
        TestInfrastructureHelper.getDebeziumContainer().ensureConnectorTaskState(SOURCE_CONNECTOR_NAME, 0, Connector.State.RUNNING);
        try {
            TestInfrastructureHelper.getDebeziumContainer().waitForStreamingRunning("mysql", config.getString(CommonConnectorConfig.TOPIC_PREFIX));
            Thread.sleep(1_000L); // ensure all topics are properly created and filled with data
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        TestInfrastructureHelper.getDebeziumContainer().deleteConnector(SOURCE_CONNECTOR_NAME);
    }

    private Configuration.Builder mongodbSinkConfig() {
        return Configuration.create()
                .with("connector.class", MongoDbSinkConnector.class)
                .with(MongoDbSinkConnectorConfig.CONNECTION_STRING, getMongoDbDeployment().getConnectionString())
                .with(SinkConnectorConfig.TOPICS_REGEX_CONFIG, "dbserver1\\.inventory\\..*")
                .with(MongoDbSinkConnectorConfig.SINK_DATABASE, DATABASE_NAME)
                .with("openlineage.integration.enabled", true)
                .with("openlineage.integration.config.file.path", getClass().getClassLoader().getResource("openlineage/openlineage.yml").getPath())
                .with("openlineage.integration.job.description", "This connector does cdc for products")
                .with("openlineage.integration.job.tags", "env=prod,team=cdc")
                .with("openlineage.integration.job.owners", "Mario=maintainer,John Doe=Data scientist");
    }

    default void startSink(Function<Configuration.Builder, Configuration.Builder> custConfig) {
        TestInfrastructureHelper.setupDebeziumContainer(Module.version(), null, TestInfrastructureHelper.parseDebeziumVersion(Module.version()));
        TestInfrastructureHelper.startContainers(DATABASE.DEBEZIUM_ONLY);
        final Configuration config = custConfig.apply(mongodbSinkConfig()).build();
        TestInfrastructureHelper.getDebeziumContainer().registerConnector(SINK_CONNECTOR_NAME, ConnectorConfiguration.from(config.asMap()));
        TestInfrastructureHelper.getDebeziumContainer().ensureConnectorTaskState(SINK_CONNECTOR_NAME, 0, Connector.State.RUNNING);
    }

    static void stopContainers(MongoDbDeployment mongo) {
        TestInfrastructureHelper.stopContainers();
        if (null != mongo) {
            mongo.stop();
        }
    }

    default List<String> listCollections(String dbName) {
        try (var client = TestHelper.connect(getMongoDbDeployment())) {
            MongoDatabase db1 = client.getDatabase(dbName);
            var collections = db1.listCollectionNames();
            collections.forEach((var collectionName) -> {
                LOGGER.info("Found collection '{}' from database '{}'", collectionName, dbName);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("with documents:");
                    db1.getCollection(collectionName).find().forEach((var doc) -> LOGGER.debug("Found document: {}", doc));
                }
            });
            return collections.into(new ArrayList<>());
        }
    }

    default void checkSinkConnectorWritesRecords() {
        startSink(Function.identity());
        Awaitility.await()
                .atMost(TestHelper.waitTimeForRecords() * 5L, TimeUnit.SECONDS)
                .until(() -> listCollections(DATABASE_NAME).size() >= 6);
        List<String> collections = listCollections(DATABASE_NAME);
        LOGGER.debug("List collections: {}", Arrays.toString(collections.toArray()));
        Assertions.assertThat(listCollections(DATABASE_NAME)).hasSize(6);
        Assertions.assertThat(collections).containsExactlyInAnyOrder("dbserver1_inventory_addresses", "dbserver1_inventory_orders", "dbserver1_inventory_customers",
                "dbserver1_inventory_products_on_hand", "dbserver1_inventory_geom", "dbserver1_inventory_products");
    }
}
