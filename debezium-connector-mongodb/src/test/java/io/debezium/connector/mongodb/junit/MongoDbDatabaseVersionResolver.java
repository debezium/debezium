/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.junit;

import java.util.List;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.MongoDbTaskContext;
import io.debezium.connector.mongodb.TestHelper;
import io.debezium.junit.DatabaseVersionResolver;

/**
 * Implementation of {@link DatabaseVersionResolver} specific for MySQL.
 *
 * @author Xinbin Huang
 */
public class MongoDbDatabaseVersionResolver implements DatabaseVersionResolver {

    public DatabaseVersion getVersion() {
        Configuration config = TestHelper.getConfiguration();
        MongoDbTaskContext context = new MongoDbTaskContext(config);

        List<Integer> version = TestHelper.getVersionArray(TestHelper.primary(context), "mongo1");

        return new DatabaseVersion(version.get(0), version.get(1), version.get(2));
    }

}
