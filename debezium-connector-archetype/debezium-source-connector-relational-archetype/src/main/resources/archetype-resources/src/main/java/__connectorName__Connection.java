/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;

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

    /**
     * Tells whether the position stored in the offset can still be read from the source's change log.
     * The task calls this on startup with the restored offset; returning {@code false} makes the
     * connector fail, or snapshot again, depending on the snapshot mode, instead of streaming from a
     * position the source has already discarded.
     */
    public boolean validateLogPosition(Partition partition, OffsetContext offset, CommonConnectorConfig config) {
        // TODO: compare the stored position with the oldest position the source still retains (for
        // example the first available log file or sequence number). Returning true skips the check.
        return true;
    }
}
