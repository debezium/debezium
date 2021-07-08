/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.connectors;

public interface ConnectorSetupFixture extends ConnectorDecoratorFixture {

    void setupConnector() throws Exception;

    void teardownConnector() throws Exception;
}
