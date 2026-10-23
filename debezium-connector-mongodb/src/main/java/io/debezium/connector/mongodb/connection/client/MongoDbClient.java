/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection.client;

import java.util.concurrent.atomic.AtomicBoolean;

import com.mongodb.client.MongoClient;

/**
 * Owns a native client and releases its factory's authentication resources after the client closes.
 */
public final class MongoDbClient implements AutoCloseable {
    private final MongoClient client;
    private final Runnable onClose;
    private final AtomicBoolean closed = new AtomicBoolean();

    public MongoDbClient(MongoClient client) {
        this(client, () -> {
        });
    }

    MongoDbClient(MongoClient client, Runnable onClose) {
        this.client = client;
        this.onClose = onClose;
    }

    public MongoClient getClient() {
        return client;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        // The driver acquires interruptible locks while closing its cluster. An existing interrupt
        // would otherwise abort cleanup after the driver has marked the client as closed.
        final boolean interrupted = Thread.interrupted();
        try {
            try {
                client.close();
            }
            catch (RuntimeException | Error clientFailure) {
                try {
                    onClose.run();
                }
                catch (RuntimeException | Error resourceFailure) {
                    clientFailure.addSuppressed(resourceFailure);
                }
                throw clientFailure;
            }
            onClose.run();
        }
        finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
