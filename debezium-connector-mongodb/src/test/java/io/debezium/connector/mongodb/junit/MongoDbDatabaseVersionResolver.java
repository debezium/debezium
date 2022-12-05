/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.junit;

import static io.debezium.connector.mongodb.TestHelper.MONGO_VERSION;

import io.debezium.junit.DatabaseVersionResolver;

/**
 * Implementation of {@link DatabaseVersionResolver} specific for MySQL.
 *
 * @author Xinbin Huang
 */
public class MongoDbDatabaseVersionResolver implements DatabaseVersionResolver {

    public DatabaseVersion getVersion() {
        return new DatabaseVersion(MONGO_VERSION.get(0), MONGO_VERSION.get(1), MONGO_VERSION.get(2));
    }

}
