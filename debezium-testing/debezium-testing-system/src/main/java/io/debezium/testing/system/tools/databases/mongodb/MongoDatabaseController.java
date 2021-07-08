/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb;

import io.debezium.testing.system.tools.databases.DatabaseController;

public interface MongoDatabaseController extends DatabaseController<MongoDatabaseClient> {

    MongoDatabaseClient getDatabaseClient(String username, String password, String authSource);
}
