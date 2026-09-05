/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

/**
 * JDBC connection to the ${connectorName} database.
 *
 * <p>Used to read the table structure on startup and to read rows during the snapshot.
 * Adjust the URL pattern and the identifier quoting characters to match your database and
 * JDBC driver, and add the driver dependency to the project's pom.xml.
 */
public class ${connectorName}Connection extends JdbcConnection {

    // The hostname, port, dbname, username, and password placeholders are filled from the
    // database.* connector configuration when the connection is opened.
    private static final String URL_PATTERN = "jdbc:${connectorName.toLowerCase()}://#[[${hostname}:${port}/${dbname}]]#";

    public ${connectorName}Connection(JdbcConfiguration config) {
        // The last two arguments are the opening and closing identifier-quoting characters;
        // change them if your database does not quote identifiers with double quotes.
        super(config, JdbcConnection.patternBasedFactory(URL_PATTERN), "\"", "\"");
    }
}
