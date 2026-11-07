/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.jdbc;

import java.sql.SQLException;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.binlog.jdbc.BinlogFieldReader;
import io.debezium.connector.binlog.jdbc.ConnectionConfiguration;
import io.debezium.connector.mariadb.gtid.MariaDbGtidSet;
import io.debezium.connector.mariadb.gtid.MariaDbGtidSet.MariaDbGtid;

/**
 * A concrete implementation of {@link BinlogConnectorConnection} for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbConnection extends BinlogConnectorConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(MariaDbConnection.class);
    private static final int BINLOG_EVENTS_PAGE_SIZE = 10_000;

    public MariaDbConnection(ConnectionConfiguration configuration, BinlogFieldReader fieldReader) {
        super(configuration, fieldReader);
    }

    @Override
    public boolean isGtidModeEnabled() {
        // Always has GTID enabled.
        // GTID_STRICT_MODE can be enabled or disabled.
        return true;
    }

    @Override
    public GtidSet knownGtidSet() {
        // MariaDB does not store the executed GTID details in the SHOW MASTER STATUS output like MySQL;
        // however, instead makes this information available as a variable. The GTID_BINLOG_POS gives
        // the current GTID position of the binary log and can therefore be considered the equivalent to
        // MySQL's executed GTID set.
        try {
            return queryAndMap("SHOW GLOBAL VARIABLES LIKE 'GTID_BINLOG_POS'", rs -> {
                if (rs.next()) {
                    return new MariaDbGtidSet(rs.getString(2));
                }
                return new MariaDbGtidSet("");
            });
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while looking at GTID_BINLOG_POS: ", e);
        }
    }

    @Override
    public GtidSet subtractGtidSet(GtidSet set1, GtidSet set2) {
        return set1.subtract(set2);
    }

    @Override
    public GtidSet purgedGtidSet() {
        // The MariaDB community mentioned we could get the purged GTID values from the GTID_LIST_EVENT; however,
        // this value is only available after we connect and would require a temporary binlog connection to get
        // the data, so for now simply returning an empty set until we split the code base.
        return new MariaDbGtidSet("");
    }

    @Override
    public GtidSet filterGtidSet(Predicate<String> gtidSourceFilter, String offsetGtids, GtidSet availableServerGtidSet, GtidSet purgedServerGtidSet) {
        String gtidStr = offsetGtids;
        if (gtidStr == null) {
            return null;
        }
        LOGGER.info("Attempting to generate a filtered GTID set");
        LOGGER.info("GTID set from previous recorded offset: {}", gtidStr);
        MariaDbGtidSet filteredGtidSet = new MariaDbGtidSet(gtidStr);
        if (gtidSourceFilter != null) {
            filteredGtidSet = (MariaDbGtidSet) filteredGtidSet.retainAll(gtidSourceFilter);
            LOGGER.info("GTID set after applying GTID source includes/excludes to previous recorded offset: {}", filteredGtidSet);
        }
        LOGGER.info("GTID set available on server: {}", availableServerGtidSet);

        final MariaDbGtidSet knownGtidSet = filteredGtidSet;
        LOGGER.info("Using first available positions for new GTID channels");
        final GtidSet relevantAvailableServerGtidSet = (gtidSourceFilter != null) ? availableServerGtidSet.retainAll(gtidSourceFilter) : availableServerGtidSet;
        LOGGER.info("Relevant GTID set available on server: {}", relevantAvailableServerGtidSet);

        GtidSet mergedGtidSet = relevantAvailableServerGtidSet
                .retainAll(serverId -> {
                    // ServerId in this context is "<domain-id>-<server-id>"
                    final MariaDbGtid compliantGtid = MariaDbGtid.parse(serverId + "-0");
                    return knownGtidSet.forGtidStream(compliantGtid) != null;
                })
                .with(purgedServerGtidSet)
                .with(filteredGtidSet);

        LOGGER.info("Final merged GTID set to use when connecting to MariaDB: {}", mergedGtidSet);
        return mergedGtidSet;
    }

    /**
     * Returns the GTID position the server had reached at {@code binlogPosition} in {@code binlogFilename}.
     * <p>
     * {@code BINLOG_GTID_POS} counts a transaction as soon as its GTID event lies before the position, so it is
     * asked for the start of the transaction the position falls inside, which is then read again rather than skipped.
     *
     * @param binlogFilename the binlog file, which must still exist on the server
     * @param binlogPosition the position within that file
     * @return the GTID position, empty if nothing was executed before it
     */
    public String gtidSetAtPosition(String binlogFilename, long binlogPosition) {
        if (binlogFilename.indexOf('\'') >= 0 || binlogFilename.indexOf('\\') >= 0) {
            throw new DebeziumException("Invalid binlog filename: " + binlogFilename);
        }
        final String gtidSet;
        try {
            if (isInnoDbBinlog()) {
                // File offsets are not meaningful in the InnoDB-based binary log and BINLOG_GTID_POS is not available.
                throw new DebeziumException("A binlog file and position cannot be resolved with the InnoDB-based binary log; use a GTID set");
            }
            final long transactionStart = transactionStart(binlogFilename, binlogPosition);
            gtidSet = prepareQueryAndMap("SELECT BINLOG_GTID_POS(?, ?)",
                    ps -> {
                        ps.setString(1, binlogFilename);
                        ps.setLong(2, transactionStart);
                    },
                    rs -> rs.next() ? rs.getString(1) : null);
        }
        catch (SQLException e) {
            throw new DebeziumException("Unable to resolve the GTID position at binlog file '" + binlogFilename + "' position " + binlogPosition, e);
        }
        if (gtidSet == null) {
            // BINLOG_GTID_POS returns NULL when the file does not exist or the position is not an event boundary.
            throw new DebeziumException("No GTID position at binlog file '" + binlogFilename + "' position " + binlogPosition);
        }
        LOGGER.info("GTID position at binlog file '{}' position {}: {}", binlogFilename, binlogPosition, gtidSet);
        return gtidSet;
    }

    private long transactionStart(String binlogFilename, long binlogPosition) throws SQLException {
        final TransactionStart finder = new TransactionStart(binlogPosition);
        // End_log_pos can be 0 inside MariaDB transactions, so each page restarts at the last event read and skips it.
        long from = 4;
        int skip = 0;
        while (true) {
            final String query = String.format("SHOW BINLOG EVENTS IN '%s' FROM %d LIMIT %d, %d",
                    binlogFilename, from, skip, BINLOG_EVENTS_PAGE_SIZE);
            final boolean morePages = queryAndMap(query, rs -> {
                int rows = 0;
                while (rs.next()) {
                    rows++;
                    if (!finder.accept(rs.getString("Event_type"), rs.getLong("Pos"))) {
                        return false;
                    }
                }
                return rows == BINLOG_EVENTS_PAGE_SIZE;
            });
            if (!morePages) {
                return finder.transactionStart();
            }
            from = nextPageStart(binlogFilename, from, finder.lastEventPosition());
            skip = 1;
        }
    }

    static long nextPageStart(String binlogFilename, long from, long lastEventPosition) {
        if (lastEventPosition <= from) {
            throw new DebeziumException("Binlog event positions in '" + binlogFilename + "' do not advance past " + from);
        }
        return lastEventPosition;
    }

    private boolean isInnoDbBinlog() throws SQLException {
        return queryAndMap("SHOW GLOBAL VARIABLES LIKE 'binlog_storage_engine'",
                rs -> rs.next() && "innodb".equalsIgnoreCase(rs.getString(2)));
    }

    /**
     * Finds, from the events of one binlog file in file order, the position {@code BINLOG_GTID_POS} has to be asked for.
     */
    static final class TransactionStart {

        private final long position;
        private long openTransactionStart = -1;
        private long lastEventPosition = 4;
        private Long result;

        TransactionStart(long position) {
            this.position = position;
        }

        /**
         * Accepts the next event of the file.
         *
         * @return {@code false} once the first event at or after the position has been seen
         */
        boolean accept(String eventType, long eventPosition) {
            final boolean boundary = isTransactionBoundary(eventType);
            if (eventPosition >= position) {
                if (eventPosition == position && boundary) {
                    result = position;
                }
                else {
                    // Inside a transaction, or inside an event: go back to the start of the open transaction.
                    result = openTransactionStart >= 0 ? openTransactionStart : 4;
                }
                return false;
            }
            if (boundary) {
                openTransactionStart = "gtid".equalsIgnoreCase(eventType) ? eventPosition : -1;
            }
            lastEventPosition = eventPosition;
            return true;
        }

        long lastEventPosition() {
            return lastEventPosition;
        }

        long transactionStart() {
            // Past the last event the position is the end of the file, or beyond it, which BINLOG_GTID_POS rejects.
            return result != null ? result : position;
        }

        private static boolean isTransactionBoundary(String eventType) {
            return "gtid".equalsIgnoreCase(eventType) || "rotate".equalsIgnoreCase(eventType) || "stop".equalsIgnoreCase(eventType);
        }
    }
}
