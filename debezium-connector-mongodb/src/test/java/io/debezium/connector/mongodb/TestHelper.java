/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoDatabase;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.ConnectionContext.MongoPrimary;

/**
 * A common test configuration options
 *
 * @author Jiri Pechanec
 *
 */
public class TestHelper {
    protected final static Logger logger = LoggerFactory.getLogger(TestHelper.class);

    public static Configuration getConfiguration() {
        return Configuration.fromSystemProperties("connector.").edit()
                .withDefault(MongoDbConnectorConfig.HOSTS, "rs0/localhost:27017")
                .withDefault(MongoDbConnectorConfig.AUTO_DISCOVER_MEMBERS, false)
                .withDefault(MongoDbConnectorConfig.LOGICAL_NAME, "mongo1").build();
    }

    public static void cleanDatabase(MongoPrimary primary, String dbName) {
        primary.execute("clean-db", mongo -> {
            MongoDatabase db1 = mongo.getDatabase(dbName);
            db1.listCollectionNames().forEach((Consumer<String>) ((String x) -> {
                logger.info("Removing collection '{}' from database '{}'", x, dbName);
                db1.getCollection(x).drop();
            }));
        });
    }
}
