/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases;

public interface DatabaseSetupFixture {

    void setupDatabase() throws Exception;

    void teardownDatabase() throws Exception;
}
