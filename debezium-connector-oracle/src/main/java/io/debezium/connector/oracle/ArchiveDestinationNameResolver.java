/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.util.Collect;
import io.debezium.util.Strings;

/**
 * A resolver that takes a collection of configured archive destination names and determines
 * which destination name should be used by the connector.
 *
 * @author Chris Cranford
 */
public class ArchiveDestinationNameResolver {

    private final Logger LOGGER = LoggerFactory.getLogger(ArchiveDestinationNameResolver.class);

    private final List<String> destinationNames;
    private ResolvedDestinationName destinationName;

    public ArchiveDestinationNameResolver(List<String> destinationNames) {
        this.destinationNames = destinationNames;
        this.destinationName = ResolvedDestinationName.unresolved();
    }

    /**
     * Validates the archive destination names.
     *
     * @param connection the database connection, should not be {@code null}
     */
    public void validate(OracleConnection connection) {
        if (!destinationName.resolved()) {
            destinationName = resolveDestinationName(connection);
        }

        try {
            if (!Strings.isNullOrEmpty(destinationName.value())) {
                if (!connection.isArchiveLogDestinationValid(destinationName.value())) {
                    LOGGER.warn("Archive log destination '{}' may not be valid, please check the database.", destinationName.value());
                }
            }
            else if (!connection.isOnlyOneArchiveLogDestinationValid()) {
                LOGGER.warn("There are multiple valid archive log destinations. " +
                        "Please add '{}' to the connector configuration to avoid log availability problems.",
                        OracleConnectorConfig.ARCHIVE_DESTINATION_NAME.name());
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Error while checking validity of archive log configuration", e);
        }
    }

    /**
     * Gets the destination name to be used.
     *
     * @param connection the database connection, should not be {@code null}
     * @return the destination name to be used, may be {@code null}
     */
    public String getDestinationName(OracleConnection connection) {
        if (!destinationName.resolved()) {
            destinationName = resolveDestinationName(connection);
        }
        return destinationName.value();
    }

    private ResolvedDestinationName resolveDestinationName(OracleConnection connection) {
        try {
            if (!Collect.isNullOrEmpty(destinationNames)) {
                for (String destinationName : destinationNames) {
                    final ParsedDestinationName parsedName = parseDestinationName(destinationName);
                    if (!Strings.isNullOrBlank(parsedName.hostName)) {
                        if (!connection.isConnectedToInstanceHostname(parsedName.hostName)) {
                            LOGGER.info("Ignoring destination {}, instance hostname {} mismatch.", parsedName.destinationName, parsedName.hostName);
                            continue;
                        }
                    }
                    if (connection.isArchiveLogDestinationValid(parsedName.destinationName)) {
                        LOGGER.info("Using archive destination {}", parsedName.destinationName);
                        return ResolvedDestinationName.resolved(parsedName.destinationName);
                    }
                }

                LOGGER.warn("No valid archive destination detected in '{}'", destinationNames);
                return ResolvedDestinationName.resolved(destinationNames.stream().findFirst().get());
            }

            // Fallback to using default behavior
            return ResolvedDestinationName.resolved(null);
        }
        catch (SQLException e) {
            throw new DebeziumException("Error while checking validity of archive destination configuration", e);
        }
    }

    /**
     * Parses a given destination name value. The value should be given in one of two formats:
     * <ul>
     *     <li>LOG_ARCHIVE_DEST_n</li>
     *     <li>hostname:LOG_ARCHIVE_DEST_n</li>
     * </ul>
     *
     * @param destinationName the destination name to be parsed
     * @return the parsed destination name
     */
    private ParsedDestinationName parseDestinationName(String destinationName) {
        if (!Strings.isNullOrBlank(destinationName) && destinationName.contains(":")) {
            final String[] parts = destinationName.split(":");
            if (parts.length != 2) {
                throw new DebeziumException("Invalid archive.destination.name format detected: " + destinationName);
            }
            return new ParsedDestinationName(parts[0], parts[1]);
        }
        return new ParsedDestinationName(null, destinationName);
    }

    private record ResolvedDestinationName(boolean resolved, String value) {
        public static ResolvedDestinationName unresolved() {
            return new ResolvedDestinationName(false, null);
        }

        public static ResolvedDestinationName resolved(String value) {
            return new ResolvedDestinationName(true, value);
        }
    }

    private record ParsedDestinationName(String hostName, String destinationName) {
    }
}
