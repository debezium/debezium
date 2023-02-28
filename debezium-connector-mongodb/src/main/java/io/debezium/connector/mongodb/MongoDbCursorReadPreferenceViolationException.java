/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import com.mongodb.connection.ServerDescription;

public class MongoDbCursorReadPreferenceViolationException extends RuntimeException {
    private final ServerDescription server;

    public MongoDbCursorReadPreferenceViolationException(ServerDescription server) {
        this.server = server;
    }

    public ServerDescription getServer() {
        return server;
    }

}
