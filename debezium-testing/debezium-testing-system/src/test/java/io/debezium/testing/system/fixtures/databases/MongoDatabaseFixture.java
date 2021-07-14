/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases;

import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseController;

public interface MongoDatabaseFixture extends DatabaseRuntimeFixture<MongoDatabaseController> {
}
