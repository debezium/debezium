/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.binlog.jdbc.BinlogFieldReader;
import io.debezium.connector.mysql.gtid.MySqlGtidSet;
import io.debezium.pipeline.source.spi.ServerRoleObservation;

/**
 * An {@link BinlogConnectorConnection} to be used with MySQL.
 *
 * @author Jiri Pechanec, Randell Hauch, Chris Cranford
 */
public class MySqlConnection extends BinlogConnectorConnection {

    public static final String BINARY_LOG_STATUS_STATEMENT = "SHOW BINARY LOG STATUS";

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlConnection.class);

    private final String binaryLogStatusStatement;
    // Whether the server is Amazon Aurora MySQL, read once for this connection
    private Boolean aurora;

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
    public boolean isServerRoleObservable() {
        return true;
    }

    @Override
    public ServerRoleObservation observeServerRole() throws SQLException {
        final boolean[] readOnly = queryAndMap("SELECT @@read_only, @@super_read_only", rs -> {
            if (rs.next()) {
                return new boolean[]{ rs.getBoolean(1), rs.getBoolean(2) };
            }
            throw new SQLException("Unable to read whether the server is read-only");
        });
        if (aurora == null) {
            aurora = queryAndMap("SHOW GLOBAL VARIABLES LIKE 'aurora_version'", rs -> rs.next());
        }
        final Boolean auroraReader = aurora ? queryAndMap("SELECT @@innodb_read_only", rs -> rs.next() && rs.getBoolean(1)) : null;
        final String groupReplicationRole = queryAndMap(
                "SELECT MEMBER_ROLE FROM performance_schema.replication_group_members WHERE MEMBER_ID = @@server_uuid AND MEMBER_STATE = 'ONLINE'",
                rs -> rs.next() ? rs.getString(1) : null);
        final List<String> replicationSources = queryAndMap(
                "SELECT c.HOST, c.PORT FROM performance_schema.replication_connection_status s "
                        + "JOIN performance_schema.replication_connection_configuration c ON c.CHANNEL_NAME = s.CHANNEL_NAME "
                        + "WHERE s.SERVICE_STATE IN ('ON', 'CONNECTING') AND s.CHANNEL_NAME NOT LIKE 'group_replication%'",
                rs -> {
                    final List<String> sources = new ArrayList<>();
                    while (rs.next()) {
                        sources.add(rs.getString(1) + ":" + rs.getInt(2));
                    }
                    return sources;
                });
        return new MySqlServerRoleFacts(readOnly[0], readOnly[1], auroraReader, groupReplicationRole, replicationSources).toObservation();
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
        return mergedGtidSet;
    }
}
