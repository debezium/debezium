/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection;

import com.mongodb.MongoClientSettings.Builder;
import com.mongodb.MongoCredential;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;

public class DefaultMongoDbAuthProvider implements MongoDbAuthProvider {

    private MongoDbConnectorConfig connectorConfig;

    @Override
    public void init(Configuration config) {
        this.connectorConfig = new MongoDbConnectorConfig(config);
    }

    @Override
    public Builder addAuthConfig(Builder settings) {
        // Use credential if provided as properties
        var user = connectorConfig.getUser();
        var password = connectorConfig.getPassword();
        var authSource = connectorConfig.getAuthSource();

        if (user != null || password != null) {
            settings.credential(MongoCredential.createCredential(user, authSource, password != null ? password.toCharArray() : null));
        }
        return settings;
    }
}
