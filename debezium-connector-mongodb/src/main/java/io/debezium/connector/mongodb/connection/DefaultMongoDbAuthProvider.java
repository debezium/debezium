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

    private String user;
    private String password;
    private String authSource;

    @Override
    public void init(Configuration config) {
        user = config.getString(MongoDbConnectorConfig.USER);
        password = config.getString(MongoDbConnectorConfig.PASSWORD);
        authSource = config.getString(MongoDbConnectorConfig.AUTH_SOURCE);
    }

    @Override
    public Builder addAuthConfig(Builder settings) {
        // Use credential if provided as properties

        if (user != null || password != null) {
            settings.credential(MongoCredential.createCredential(user, authSource, password != null ? password.toCharArray() : null));
        }
        return settings;
    }
}
