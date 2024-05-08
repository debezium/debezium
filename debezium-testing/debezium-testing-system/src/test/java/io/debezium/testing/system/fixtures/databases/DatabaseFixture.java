/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases;

import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.tools.databases.DatabaseController;

import fixture5.TestFixture;

public abstract class DatabaseFixture<T extends DatabaseController<?>> extends TestFixture {

    protected final Class<T> controllerType;
    protected T dbController;

    public DatabaseFixture(Class<T> controllerType, ExtensionContext.Store store) {
        super(store);
        this.controllerType = controllerType;
    }

    protected abstract T databaseController() throws Exception;

    @Override
    public void setup() throws Exception {
        dbController = databaseController();
        dbController.initialize();

        store(controllerType, dbController);
    }

    @Override
    public void teardown() throws Exception {
        if (dbController != null) {
            dbController.reload();
        }
    }
}
