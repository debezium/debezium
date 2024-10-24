/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.test.quarkus.callback;

import io.debezium.outbox.test.quarkus.remote.RemoteConnectorApi;
import io.quarkus.test.junit.callback.QuarkusTestAfterEachCallback;
import io.quarkus.test.junit.callback.QuarkusTestMethodContext;

/**
 * This class is a QuarkusTest callback that deletes the Debezium outbox connector
 * after each test method execution.
 *
 * <p>
 * It ensures that the connector is properly cleaned up after the test completes,
 * so each test starts with a fresh environment without any leftover connectors.
 * </p>
 */
public final class DeleteOutboxConnectorAfterEachCallback implements QuarkusTestAfterEachCallback {

    /**
     * Deletes the Debezium outbox connector after each test method.
     *
     * <p>
     * The method calls the {@link io.debezium.outbox.test.quarkus.remote.RemoteConnectorApi} to remove the connector from
     * the system, ensuring that the environment is reset for the next test.
     * </p>
     *
     * @param context The test method context provided by the Quarkus test framework.
     */
    @Override
    public void afterEach(final QuarkusTestMethodContext context) {
        final RemoteConnectorApi remoteConnectorApi = RemoteConnectorApi.createInstance();
        remoteConnectorApi.deleteOutboxConnector();
    }
}