/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.shared;

import org.apache.kafka.common.config.ConfigDef;

import com.mongodb.ConnectionString;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

public interface SharedMongoDbConnectorConfig {

    // MongoDb fields in Connection Group start from 1 (topic.prefix is 0)
    Field CONNECTION_STRING = Field.create("mongodb.connection.string")
            .withDisplayName("Connection String")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1))
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(SharedMongoDbConnectorConfig::validateConnectionString)
            .withDescription("Database connection string.");

    private static int validateConnectionString(Configuration config, Field field, Field.ValidationOutput problems) {
        String connectionStringValue = config.getString(field);

        if (connectionStringValue == null) {
            problems.accept(field, null, "Missing connection string");
            return 1;
        }

        try {
            new ConnectionString(connectionStringValue);
        }
        catch (Exception e) {
            problems.accept(field, connectionStringValue, "Invalid connection string");
            return 1;
        }
        return 0;
    }

    default ConnectionString resolveConnectionString(Configuration config) {
        var connectionString = config.getString(CONNECTION_STRING);
        return new ConnectionString(connectionString);
    }

}
