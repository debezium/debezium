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
public class MongoDbConnectionTest {

    private Configuration getConfig(String connectionString, boolean ssl) {
        return TestHelper.getConfiguration(connectionString).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.CONNECTION_STRING, connectionString)
                .with(MongoDbConnectorConfig.SSL_ENABLED, ssl)
                .build();
    }

    @Test
    public void shouldMaskCredentials() {
        var config = getConfig("mongodb://admin:password@localhost:27017/", false);
        var connectionContext = new ConnectionContext(config);

        var masked = connectionContext.getMaskedConnectionString();
        assertThat(masked).isEqualTo("mongodb://***:***@localhost:27017/");
    }
}
