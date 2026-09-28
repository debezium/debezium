/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.mongodb.ReadPreference;
import com.mongodb.Tag;
import com.mongodb.TagSet;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.connection.MongoDbConnectionContext;
import io.debezium.doc.FixFor;

/**
 * @author Randall Hauch
 *
 */
public class MongoDbConnectionContextTest {

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
    void shouldDefaultToPrimaryReadPreference() {
        var connectionContext = new MongoDbConnectionContext(getConfig("mongodb://localhost:27017/", false));

        assertThat(connectionContext.getConnectionString().getReadPreference()).isNull();
        try (var client = connectionContext.getMongoClient()) {
            assertThat(client.getReadPreference()).isEqualTo(ReadPreference.primary());
        }
    }

    @Test
    void shouldParseTaggedSecondaryReadPreferenceFromStandardConnectionString() {
        var connectionContext = new MongoDbConnectionContext(getConfig(
                "mongodb://localhost:27017/?readPreference=secondary&readPreferenceTags=region:east", false));

        assertThat(connectionContext.getConnectionString().getReadPreference())
                .isEqualTo(ReadPreference.secondary(new TagSet(new Tag("region", "east"))));
    }

    @Test
    void shouldParseTaggedSecondaryReadPreferenceFromSrvConnectionString() {
        var connectionContext = new MongoDbConnectionContext(getConfig(
                "mongodb+srv://cluster0.example.com/?readPreference=secondary&readPreferenceTags=region:east", false));

        assertThat(connectionContext.getConnectionString().getReadPreference())
                .isEqualTo(ReadPreference.secondary(new TagSet(new Tag("region", "east"))));
    }

    @Test
    void shouldMaskCredentials() {
        var config = getConfig("mongodb://admin:password@localhost:27017/", false);
        var connectionContext = new MongoDbConnectionContext(config);

        var masked = connectionContext.getMaskedConnectionString();
        assertThat(masked).isEqualTo("mongodb://***:***@localhost:27017/");
    }

    @Test
    @FixFor("debezium/dbz#2719")
    void shouldMaskCredentialsWithDistinctAuthSource() {
        var config = getConfig("mongodb://appuser:secret123@localhost:27017/?authSource=admin", false);
        var connectionContext = new MongoDbConnectionContext(config);

        var masked = connectionContext.getMaskedConnectionString();
        assertThat(masked).isEqualTo("mongodb://***:***@localhost:27017/?authSource=***");
    }
}
