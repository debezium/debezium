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
    private String username;
    private String password;
    private String adminDbName;

    @Override
    public void init(Configuration config) {
        username = config.getString(MongoDbConnectorConfig.USER);
        password = config.getString(MongoDbConnectorConfig.PASSWORD);
        adminDbName = config.getString(MongoDbConnectorConfig.AUTH_SOURCE);
    }

    @Override
    public Builder addAuthConfig(Builder settings) {
        // Use credential if provided as properties
        if (username != null || password != null) {
            settings.credential(MongoCredential.createCredential(username, adminDbName, password.toCharArray()));
        }

        return settings;
    }
}
