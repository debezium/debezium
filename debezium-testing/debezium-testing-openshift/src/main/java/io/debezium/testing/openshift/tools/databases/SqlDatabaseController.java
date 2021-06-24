/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.databases;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface SqlDatabaseController extends DatabaseController<SqlDatabaseClient> {
    Logger LOGGER = LoggerFactory.getLogger(SqlDatabaseController.class);

    /**
     * @return jdbc vendor connection type
     */
    String getDatabaseType();

    @Override
    default String getDatabaseUrl() {
        return "jdbc:" + getDatabaseType() + "://" + getDatabaseHostname() + ":" + getDatabasePort() + "/";
    }

    @Override
    default SqlDatabaseClient getDatabaseClient(String username, String password) {
        String databaseUrl = getDatabaseUrl();
        LOGGER.info("Creating SQL database client for '" + databaseUrl + "'");
        LOGGER.info("Using credentials '" + username + "' / '" + password + "'");
        return new SqlDatabaseClient(databaseUrl, username, password);
    }

}
