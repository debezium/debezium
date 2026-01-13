/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.util.Collect;

/**
 * A resolver that takes a collection of configured archive destination names and determines
 * which destination name should be used by the connector.
 *
 * @author Chris Cranford
 */
public class ArchiveDestinationNameResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveDestinationNameResolver.class);

    private static final String ARCHIVE_DEST_NAME_FORMAT = "LOG_ARCHIVE_DEST_%d";
    private static final int ARCHIVE_DEST_FIRST_ID = 1;
    private static final int ARCHIVE_DEST_LAST_ID = 31;

    private final List<String> configuredDestinationNames;
    private final List<String> resolvedDestinationNames;

    public ArchiveDestinationNameResolver(List<String> destinationNames) {
        this.configuredDestinationNames = destinationNames;
        this.resolvedDestinationNames = new ArrayList<>();
    }

    /**
     * Validates the archive destination names.
     *
     * @param connection the database connection, should not be {@code null}
     */
    public void validate(OracleConnection connection) {
        if (resolvedDestinationNames.isEmpty()) {
            resolveDestinations(connection);
        }

        if (resolvedDestinationNames.isEmpty()) {
            if (configuredDestinationNames.isEmpty()) {
                throw new DebeziumException("Failed to locate a local and valid archive destination in Oracle.");
            }
            throw new DebeziumException("None of the supplied archive destinations are local and valid: " + configuredDestinationNames);
        }
    }

    /**
     * Gets the destination names to be used in priority order.
     *
     * @param connection the database connection, should not be {@code null}
     * @return the destination names to be used, will never be {@code null} nor empty.
     */
    public List<String> getDestinationNames(OracleConnection connection) {
        validate(connection);

        return resolvedDestinationNames;
    }

    private void resolveDestinations(OracleConnection connection) {
        try {
            if (!Collect.isNullOrEmpty(configuredDestinationNames)) {
                resolvedDestinationNames.addAll(resolveDestinationsFromConfiguration(connection));
            }
            else {
                resolvedDestinationNames.addAll(resolveDestinationFromFallback(connection));
            }

            if (resolvedDestinationNames.size() > 1) {
                LOGGER.info("Using priority order archive destination names: {}", resolvedDestinationNames);
            }
            else if (resolvedDestinationNames.size() == 1) {
                LOGGER.info("Using archive destination name: {}", resolvedDestinationNames.get(0));
            }
            else {
                LOGGER.error("Failed to locate a valid archive destination.");
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Error while checking validity of archive destination configuration", e);
        }
    }

    /**
     * Uses the provided archive destination name configuration to locate valid destinations, retaining all
     * configured destinations in user-supplied order that are valid, e.g. both local and valid.
     *
     * @param connection database connection, should never be {@code null}
     * @return the list of matched destination names
     * @throws SQLException thrown if a database error occurs
     */
    private List<String> resolveDestinationsFromConfiguration(OracleConnection connection) throws SQLException {
        final List<String> destinationNames = new ArrayList<>();
        for (String destinationName : configuredDestinationNames) {
            if (connection.isArchiveLogDestinationValid(destinationName)) {
                destinationNames.add(destinationName);
            }
        }
        return destinationNames;
    }

    /**
     * Uses the fallback behavior, which scans all entries in {@code V$ARCHIVE_DEST_STATUS}, locating the
     * first entry that is considered valid, e.g. both local and valid.
     *
     * @param connection database connection, should never be {@code null}
     * @return the list of matched destination names, which is always either empty or a max size of 1.
     * @throws SQLException thrown if a database error occurs
     */
    private List<String> resolveDestinationFromFallback(OracleConnection connection) throws SQLException {
        final List<String> destinationNames = new ArrayList<>();
        for (int i = ARCHIVE_DEST_FIRST_ID; i <= ARCHIVE_DEST_LAST_ID; i++) {
            final String destinationName = ARCHIVE_DEST_NAME_FORMAT.formatted(i);
            LOGGER.info("Attempting to locate valid archive destination at {}", destinationName);
            if (connection.isArchiveLogDestinationValid(destinationName)) {
                destinationNames.add(destinationName);
                break;
            }
        }
        return destinationNames;
    }

}
