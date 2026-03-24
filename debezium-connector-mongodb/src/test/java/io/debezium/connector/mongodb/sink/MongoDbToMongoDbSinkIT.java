/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoDatabase;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.AbstractMongoConnectorIT;
import io.debezium.connector.mongodb.Module;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.TestHelper;
import io.debezium.connector.mongodb.sink.junit.NetworkIsolatedMongoDbDatabaseProvider;
import io.debezium.junit.RequiresAssemblyProfile;
import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.MongoDbDeployment;
import io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper;
import io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper.DATABASE;
import io.debezium.testing.testcontainers.util.DockerUtils;

@RequiresAssemblyProfile
public class MongoDbToMongoDbSinkIT extends AbstractMongoConnectorIT {

    protected static MongoDbDeployment mongo;
    
    private static final String DATABASE_NAME_SOURCE = "inventory_source";
    private static final String DATABASE_NAME_TARGET = "inventory_target";
    private static final String SOURCE_CONNECTOR_NAME = "mongodb-source";
    private static final String SINK_CONNECTOR_NAME = "mongodb-sink";

    @BeforeAll
    static void beforeAll() {
        DockerUtils.enableFakeDnsIfRequired();
        mongo = new NetworkIsolatedMongoDbDatabaseProvider(TestInfrastructureHelper.getNetwork()).dockerReplicaSet();
        mongo.start();
    }

    @AfterAll
    static void afterAll() {
        TestInfrastructureHelper.stopContainers();
        if (mongo != null) {
            mongo.stop();
        }
        DockerUtils.disableFakeDns();
    }

    @BeforeEach
    public void beforeEach() {
        // Clean out any existing data
        TestHelper.cleanDatabase(mongo, DATABASE_NAME_SOURCE);
        TestHelper.cleanDatabase(mongo, DATABASE_NAME_TARGET);
        
        // Insert some initial data into source db
        insertDocuments(DATABASE_NAME_SOURCE, "customers", 
            Document.parse("{ \"_id\": 1001, \"first_name\": \"Sally\", \"last_name\": \"Thomas\", \"email\": \"sally.thomas@acme.com\" }"),
            Document.parse("{ \"_id\": 1002, \"first_name\": \"George\", \"last_name\": \"Bailey\", \"email\": \"gbailey@foobar.com\" }")
        );
    }

    @AfterEach
    public void afterEach() {
        TestHelper.cleanDatabase(mongo, DATABASE_NAME_SOURCE);
        TestHelper.cleanDatabase(mongo, DATABASE_NAME_TARGET);
    }

    // Shadowing the connect method from AbstractMongoConnectorIT to use our network isolated mongo
    @Override
    protected com.mongodb.client.MongoClient connect() {
        return TestHelper.connect(mongo);
    }

    private Configuration.Builder mongoDbSourceConfig() {
        return Configuration.create()
                .with("connector.class", MongoDbConnector.class.getName())
                .with(MongoDbConnectorConfig.CONNECTION_STRING, mongo.getConnectionString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongoserver")
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE_NAME_SOURCE)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, DATABASE_NAME_SOURCE + ".customers")
                .with(MongoDbConnectorConfig.CAPTURE_MODE, "change_streams_update_full");
    }

    private void sendSourceData() {
        TestInfrastructureHelper.defaultDebeziumContainer(Module.version());
        TestInfrastructureHelper.startContainers(DATABASE.DEBEZIUM_ONLY);
        
        final Configuration config = mongoDbSourceConfig().build();
        TestInfrastructureHelper.getDebeziumContainer().registerConnector(SOURCE_CONNECTOR_NAME, ConnectorConfiguration.from(config.asMap()));
        TestInfrastructureHelper.getDebeziumContainer().ensureConnectorTaskState(SOURCE_CONNECTOR_NAME, 0, Connector.State.RUNNING);
        
        try {
            TestInfrastructureHelper.getDebeziumContainer().waitForStreamingRunning("mongodb", config.getString(CommonConnectorConfig.TOPIC_PREFIX));
            
            // Give Kafka topics a second to start digesting
            Thread.sleep(1_000L);
            
            // Insert another document after streaming started
            insertDocuments(DATABASE_NAME_SOURCE, "customers", 
                Document.parse("{ \"_id\": 1003, \"first_name\": \"Edward\", \"last_name\": \"Walker\", \"email\": \"ed@walker.com\" }"));
            
            // Wait for topics to be filled
            Thread.sleep(3_000L);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        TestInfrastructureHelper.getDebeziumContainer().deleteConnector(SOURCE_CONNECTOR_NAME);
    }

    private Configuration.Builder mongodbSinkConfig() {
        return Configuration.create()
                .with("connector.class", MongoDbSinkConnector.class.getName())
                .with(MongoDbSinkConnectorConfig.CONNECTION_STRING, mongo.getConnectionString())
                .with(SinkConnectorConfig.TOPICS_REGEX_CONFIG, "mongoserver\\." + DATABASE_NAME_SOURCE + "\\..*")
                .with(MongoDbSinkConnectorConfig.SINK_DATABASE, DATABASE_NAME_TARGET);
    }

    private void startSink() {
        // Debezium container should already be running from sendSourceData
        final Configuration config = mongodbSinkConfig().build();
        TestInfrastructureHelper.getDebeziumContainer().registerConnector(SINK_CONNECTOR_NAME, ConnectorConfiguration.from(config.asMap()));
        TestInfrastructureHelper.getDebeziumContainer().ensureConnectorTaskState(SINK_CONNECTOR_NAME, 0, Connector.State.RUNNING);
    }

    @Test
    void testSinkConnectorWritesMongoDbSourceRecords() {
        sendSourceData();
        startSink();
        
        String targetCollectionName = "mongoserver_" + DATABASE_NAME_SOURCE + "_customers";

        Awaitility.await()
                .atMost(TestHelper.waitTimeForRecords() * 5L, TimeUnit.SECONDS)
                .until(() -> {
                    try (var client = TestHelper.connect(mongo)) {
                        MongoDatabase db = client.getDatabase(DATABASE_NAME_TARGET);
                        var count = db.getCollection(targetCollectionName).countDocuments();
                        return count >= 3;
                    } catch (Exception e) {
                        return false;
                    }
                });
                
        // Verify target database
        try (var client = TestHelper.connect(mongo)) {
            MongoDatabase db = client.getDatabase(DATABASE_NAME_TARGET);
            var customers = db.getCollection(targetCollectionName);
            Assertions.assertThat(customers.countDocuments()).isGreaterThanOrEqualTo(3);
            
            // Ensure data structures are correctly written as MongoDB write models via our new event handler
            Document doc = customers.find(new Document("_id", 1001)).first();
            Assertions.assertThat(doc).isNotNull();
            Assertions.assertThat(doc.getString("first_name")).isEqualTo("Sally");
            Assertions.assertThat(doc.getString("last_name")).isEqualTo("Thomas");
        }
    }
}
