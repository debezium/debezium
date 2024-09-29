/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.test.quarkus.callback;

import io.debezium.outbox.test.quarkus.remote.RemoteConnectorApi;
import io.quarkus.test.junit.callback.QuarkusTestAfterEachCallback;
import io.quarkus.test.junit.callback.QuarkusTestMethodContext;

public final class DeleteOutboxConnectorAfterEachCallback implements QuarkusTestAfterEachCallback {

    @Override
    public void afterEach(final QuarkusTestMethodContext context) {
        final RemoteConnectorApi remoteConnectorApi = RemoteConnectorApi.createInstance();
        remoteConnectorApi.deleteOutboxConnector();
    }
}