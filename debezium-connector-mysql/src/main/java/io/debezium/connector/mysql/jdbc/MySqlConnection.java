/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.jdbc;

import java.sql.SQLException;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.binlog.jdbc.BinlogFieldReader;
import io.debezium.connector.mysql.gtid.MySqlGtidSet;

/**
 * An {@link BinlogConnectorConnection} to be used with MySQL.
 *
 * @author Jiri Pechanec, Randell Hauch, Chris Cranford
 */
public class MySqlConnection extends BinlogConnectorConnection {

    public static final String BINARY_LOG_STATUS_STATEMENT = "SHOW BINARY LOG STATUS";

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlConnection.class);
    private static final int BINLOG_EVENTS_PAGE_SIZE = 10_000;

    private final String binaryLogStatusStatement;

    public MySqlConnection(MySqlConnectionConfiguration connectionConfig, BinlogFieldReader fieldReader) {
        super(connectionConfig, fieldReader);

        this.binaryLogStatusStatement = initializeAndCloseOnFailure(this::resolveBinaryLogStatusStatement);
    }

    public static boolean isNetworkError(SQLException e) {
        String sqlState = e.getSQLState();
        if (sqlState == null) {
            return false;
        }

        return sqlState.equals("08001") // Cannot connect to server
                || sqlState.equals("08003") // Connection does not exist
                || sqlState.equals("08004") // Connection rejected by server
                || sqlState.equals("08006") // Connection failure
                || sqlState.equals("08S01"); // Communication link failure
    }

    private String resolveBinaryLogStatusStatement() {
        try {
            query(BINARY_LOG_STATUS_STATEMENT, rs -> {
            });
            LOGGER.info("Using '{}' to get binary log status", BINARY_LOG_STATUS_STATEMENT);
            return BINARY_LOG_STATUS_STATEMENT;
        }
        catch (SQLException e) {
            if (isNetworkError(e)) {
                throw new DebeziumException("Failed to establish connection with the database: ", e);
            }

            try {
                String version = queryAndMap("SELECT VERSION()", rs -> {
                    return rs.next() ? rs.getString(1) : "unknown";
                });

                String[] parts = version.split("\\.");
                if (parts.length < 2) {
                    LOGGER.warn("Unexpected MySQL version format: {}. Proceeding with fallback.", version);
                    return MASTER_STATUS_STATEMENT;
                }

                int major = Integer.parseInt(parts[0]); // 8
                int minor = Integer.parseInt(parts[1]); // 4

                if (major > 8 || (major == 8 && minor >= 4)) {
                    throw new DebeziumException("MySQL version " + version +
                            " should support SHOW BINARY LOG STATUS but it failed. Check database permissions or connectivity.", e);
                }

                LOGGER.info("MySQL version {} detected, falling back to '{}'", version, MASTER_STATUS_STATEMENT);
            }
            catch (SQLException ve) {
                LOGGER.warn("Could not determine MySQL version during fallback detection: {}", ve.getMessage());
            }
            catch (NumberFormatException nfe) {
                LOGGER.warn("Could not parse MySQL version string during fallback detection: {}", nfe.getMessage());
            }
            return MASTER_STATUS_STATEMENT;
        }
    }

    public String binaryLogStatusStatement() {
        return binaryLogStatusStatement;
    }

    @Override
    public boolean isGtidModeEnabled() {
        try {
            return queryAndMap("SHOW GLOBAL VARIABLES LIKE 'GTID_MODE'", rs -> {
                if (rs.next()) {
                    return "ON".equalsIgnoreCase(rs.getString(2));
                }
                return false;
            });
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while looking at GTID mode: ", e);
        }
    }

    @Override
    public GtidSet knownGtidSet() {
        try {
            return queryAndMap(binaryLogStatusStatement(), rs -> {
                if (rs.next() && rs.getMetaData().getColumnCount() > 4) {
                    return new MySqlGtidSet(rs.getString(5)); // GTID set, may be null, blank, or contain a GTID set
                }
                return new MySqlGtidSet("");
            });
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while looking at GTID mode: ", e);
        }
    }

    @Override
    public GtidSet subtractGtidSet(GtidSet set1, GtidSet set2) {
        try {
            return prepareQueryAndMap("SELECT GTID_SUBTRACT(?, ?)",
                    ps -> {
                        ps.setString(1, set1.toString());
                        ps.setString(2, set2.toString());
                    },
                    rs -> {
                        if (rs.next()) {
                            return new MySqlGtidSet(rs.getString(1));
                        }
                        return new MySqlGtidSet("");
                    });
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while executing GTID_SUBTRACT: ", e);
        }
    }

    @Override
    public GtidSet purgedGtidSet() {
        try {
            return queryAndMap("SELECT @@global.gtid_purged", rs -> {
                if (rs.next() && rs.getMetaData().getColumnCount() > 0) {
                    return new MySqlGtidSet(rs.getString(1)); // GTID set, may be null, blank, or contain a GTID set
                }
                return new MySqlGtidSet("");
            });
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while looking at gtid_purged variable: ", e);
        }
    }

    @Override
    public GtidSet filterGtidSet(Predicate<String> gtidSourceFilter, String offsetGtids, GtidSet availableServerGtidSet, GtidSet purgedServerGtidSet) {
        String gtidStr = offsetGtids;
        if (gtidStr == null) {
            return null;
        }
        LOGGER.info("Attempting to generate a filtered GTID set");
        LOGGER.info("GTID set from previous recorded offset: {}", gtidStr);
        GtidSet filteredGtidSet = new MySqlGtidSet(gtidStr);
        if (gtidSourceFilter != null) {
            filteredGtidSet = filteredGtidSet.retainAll(gtidSourceFilter);
            LOGGER.info("GTID set after applying GTID source includes/excludes to previous recorded offset: {}", filteredGtidSet);
        }
        LOGGER.info("GTID set available on server: {}", availableServerGtidSet);

        final MySqlGtidSet knownGtidSet = (MySqlGtidSet) filteredGtidSet;
        LOGGER.info("Using first available positions for new GTID channels");
        final MySqlGtidSet relevantAvailableServerGtidSet = (MySqlGtidSet) ((gtidSourceFilter != null) ? availableServerGtidSet.retainAll(gtidSourceFilter)
                : availableServerGtidSet);
        LOGGER.info("Relevant GTID set available on server: {}", relevantAvailableServerGtidSet);

        GtidSet mergedGtidSet = relevantAvailableServerGtidSet
                .retainAllKnownTsids(knownGtidSet)
                .with(purgedServerGtidSet)
                .with(filteredGtidSet);

        LOGGER.info("Final merged GTID set to use when connecting to MySQL: {}", mergedGtidSet);
        logUntrackedSources(relevantAvailableServerGtidSet, knownGtidSet, (MySqlGtidSet) purgedServerGtidSet);
        return mergedGtidSet;
    }

    /**
     * Warns about each GTID source the server has but the offset does not track, with the number of its
     * retained transactions that will be read again from the earliest available position.
     */
    private void logUntrackedSources(MySqlGtidSet availableGtidSet, MySqlGtidSet knownGtidSet, MySqlGtidSet purgedGtidSet) {
        for (MySqlGtidSet.UUIDSet available : availableGtidSet.getUUIDSets()) {
            if (knownGtidSet.forServerWithIdAndTag(available.getUUID(), available.getTag()) != null) {
                continue;
            }
            final MySqlGtidSet.UUIDSet purged = purgedGtidSet == null ? null
                    : purgedGtidSet.forServerWithIdAndTag(available.getUUID(), available.getTag());
            final MySqlGtidSet.UUIDSet retained = available.subtract(purged);
            final long count = retained.getIntervals().stream()
                    .mapToLong(interval -> interval.getEnd() - interval.getStart() + 1)
                    .sum();
            if (count > 0) {
                LOGGER.warn("GTID source {} is not in the offset; its {} retained transactions ({}) will be read from the earliest available position",
                        available.getUUID(), count, retained);
            }
        }
    }

    /**
     * Returns the GTID set the server had executed when it reached {@code binlogPosition} in {@code binlogFilename}.
     * <p>
     * This is the {@code Previous_gtids} of the file plus every transaction in the file that ends at or before the
     * position. A transaction the position falls inside is left out, so it is read again rather than skipped.
     *
     * @param binlogFilename the binlog file, which must still exist on the server
     * @param binlogPosition the position within that file
     * @return the GTID set, empty if the file carries no GTIDs
     */
    public String gtidSetAtPosition(String binlogFilename, long binlogPosition) {
        if (binlogFilename.indexOf('\'') >= 0 || binlogFilename.indexOf('\\') >= 0) {
            throw new DebeziumException("Invalid binlog filename: " + binlogFilename);
        }
        final GtidSetAtPosition resolver = new GtidSetAtPosition(binlogPosition);
        // The first event of every binlog file follows the 4-byte magic number.
        long from = 4;
        try {
            while (true) {
                final String query = String.format("SHOW BINLOG EVENTS IN '%s' FROM %d LIMIT %d",
                        binlogFilename, from, BINLOG_EVENTS_PAGE_SIZE);
                final boolean morePages = queryAndMap(query, rs -> {
                    int rows = 0;
                    while (rs.next()) {
                        rows++;
                        if (!resolver.accept(rs.getString("Event_type"), rs.getLong("Pos"), rs.getLong("End_log_pos"), rs.getString("Info"))) {
                            return false;
                        }
                    }
                    return rows == BINLOG_EVENTS_PAGE_SIZE;
                });
                if (!morePages || resolver.lastEventEnd() <= from) {
                    break;
                }
                from = resolver.lastEventEnd();
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Unable to read binlog file '" + binlogFilename + "' to resolve the GTID set at position " + binlogPosition, e);
        }
        final String gtidSet = resolver.gtidSet();
        LOGGER.info("GTID set at binlog file '{}' position {}: {}", binlogFilename, binlogPosition, gtidSet);
        return gtidSet;
    }

    /**
     * Folds the events of one binlog file, in file order, into the GTID set executed before a position.
     */
    static final class GtidSetAtPosition {

        private static final Pattern GTID_NEXT = Pattern.compile("GTID_NEXT\\s*=\\s*'([^']+)'", Pattern.CASE_INSENSITIVE);

        private final long position;
        private com.github.shyiko.mysql.binlog.GtidSet gtidSet = new com.github.shyiko.mysql.binlog.GtidSet("");
        private String openGtid;
        private long lastEventEnd;

        GtidSetAtPosition(long position) {
            this.position = position;
        }

        /**
         * Accepts the next event of the file.
         *
         * @return {@code false} once the first event at or after the position has been seen
         */
        boolean accept(String eventType, long eventPosition, long eventEnd, String info) {
            final String type = eventType == null ? "" : eventType.toLowerCase();
            if (type.equals("previous_gtids")) {
                // Multiple sources are separated by a comma and a newline.
                if (info != null && !info.isBlank()) {
                    gtidSet = new com.github.shyiko.mysql.binlog.GtidSet(info.replace("\n", "").replace("\r", "").trim());
                }
                lastEventEnd = eventEnd;
                return true;
            }
            if (type.equals("format_desc")) {
                lastEventEnd = eventEnd;
                return true;
            }
            if (eventPosition >= position) {
                if (isTransactionBoundary(type)) {
                    closeOpenTransaction();
                }
                openGtid = null;
                return false;
            }
            if (isTransactionBoundary(type)) {
                closeOpenTransaction();
                openGtid = type.startsWith("gtid") ? gtidOf(info) : null;
            }
            lastEventEnd = eventEnd;
            return true;
        }

        long lastEventEnd() {
            return lastEventEnd;
        }

        String gtidSet() {
            // At the end of the file the last transaction is complete if it ends by the position.
            closeOpenTransaction();
            return gtidSet.toString();
        }

        private void closeOpenTransaction() {
            if (openGtid != null && lastEventEnd <= position) {
                gtidSet.add(openGtid);
            }
            openGtid = null;
        }

        private static boolean isTransactionBoundary(String type) {
            return type.startsWith("gtid") || type.equals("anonymous_gtid") || type.equals("rotate") || type.equals("stop");
        }

        private static String gtidOf(String info) {
            final Matcher matcher = info == null ? null : GTID_NEXT.matcher(info);
            if (matcher == null || !matcher.find()) {
                throw new DebeziumException("Unable to read the GTID from binlog event: " + info);
            }
            return matcher.group(1);
        }
    }
}
