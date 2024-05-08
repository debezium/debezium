/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases;

public interface DatabaseController<C extends DatabaseClient<?, ?>> {

    /**
     * @return available hostname of the database
     */
    String getDatabaseHostname();

    /**
     * @return available port of the database
     */
    int getDatabasePort();

    /**
     * @return publicly available hostname of the database
     */
    String getPublicDatabaseHostname();

    /**
     * @return publicly available port of the database
     */
    int getPublicDatabasePort();

    /**
     * @return publicly available connection url of the database
     */
    String getPublicDatabaseUrl();

    /**
     * Creates database client for database using given username and password
     *
     * @param username username
     * @param password password
     * @return database client
     */
    C getDatabaseClient(String username, String password);

    /**
     * Reloads the database to initial state
     *
     * @throws InterruptedException on timing issue
     */
    void reload() throws InterruptedException;

    /**
     * Database initialisation
     * @throws InterruptedException on timing issue
     */
    default void initialize() throws InterruptedException {
        // no-op
    }

}
