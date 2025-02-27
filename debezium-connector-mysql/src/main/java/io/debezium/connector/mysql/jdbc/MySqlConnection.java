/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.jdbc;

import java.sql.SQLException;
import java.util.function.Predicate;

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

    private final String binaryLogStatusStatement;

    public MySqlConnection(MySqlConnectionConfiguration connectionConfig, BinlogFieldReader fieldReader) {
        super(connectionConfig, fieldReader);

        try {
            query(BINARY_LOG_STATUS_STATEMENT, rs -> {
            });
        }
        catch (SQLException e) {
            LOGGER.info("Using '{}' to get binary log status", MASTER_STATUS_STATEMENT);
            binaryLogStatusStatement = MASTER_STATUS_STATEMENT;
            return;
        }
        LOGGER.info("Using '{}' to get binary log status", BINARY_LOG_STATUS_STATEMENT);
        binaryLogStatusStatement = BINARY_LOG_STATUS_STATEMENT;
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

        final GtidSet knownGtidSet = filteredGtidSet;
        LOGGER.info("Using first available positions for new GTID channels");
        final GtidSet relevantAvailableServerGtidSet = (gtidSourceFilter != null) ? availableServerGtidSet.retainAll(gtidSourceFilter) : availableServerGtidSet;
        LOGGER.info("Relevant GTID set available on server: {}", relevantAvailableServerGtidSet);

        GtidSet mergedGtidSet = relevantAvailableServerGtidSet
                .retainAll(uuid -> ((MySqlGtidSet) knownGtidSet).forServerWithId(uuid) != null)
                .with(purgedServerGtidSet)
                .with(filteredGtidSet);

        LOGGER.info("Final merged GTID set to use when connecting to MySQL: {}", mergedGtidSet);
        return mergedGtidSet;
    }
}
