/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.connection.ConnectionContext;

/**
 * @author Randall Hauch
 *
 */
public class MongoDbConnectorWithConnectionStringIT extends AbstractMongoConnectorIT {

    private Configuration getConfig(String connectionString, boolean ssl) {
        var properties = TestHelper.getConfiguration(mongo).asProperties();
        properties.remove(MongoDbConnectorConfig.HOSTS.name());

        return Configuration.from(properties).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.CONNECTION_STRING, connectionString)
                .with(MongoDbConnectorConfig.SSL_ENABLED, ssl)
                .build();
    }

    @Test
    public void shouldMaskCredentials() {
        config = getConfig("mongodb://admin:password@localhost:27017/", false);
        var connectionContext = new ConnectionContext(config);

        var masked = connectionContext.maskedConnectionSeed();
        assertThat(masked).isEqualTo("mongodb://***:***@localhost:27017/");
    }
}
