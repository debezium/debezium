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
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.binlog.jdbc.BinlogFieldReader;
import io.debezium.connector.binlog.jdbc.ConnectionConfiguration;
import io.debezium.connector.mariadb.gtid.MariaDbGtidSet;
import io.debezium.connector.mariadb.gtid.MariaDbGtidSet.MariaDbGtid;
import io.debezium.util.Strings;

/**
 * A concrete implementation of {@link BinlogConnectorConnection} for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbConnection extends BinlogConnectorConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(MariaDbConnection.class);
    private static final String GTID_LIST_EVENT_TYPE = "Gtid_list";

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
        // MariaDB does not expose a purged GTID set like MySQL's GTID_PURGED. Whether the binlogs needed by
        // the connector were purged is checked separately, see earliestAvailableGtidSet(). This stays empty
        // because a non-empty value would change how the GTID set used for streaming is built.
        return new MariaDbGtidSet("");
    }

    @Override
    public boolean isBinlogPositionAvailable(BinlogConnectorConfig config, String gtid, String binlogFileName) {
        if (!super.isBinlogPositionAvailable(config, gtid, binlogFileName)) {
            return false;
        }
        if (Strings.isNullOrBlank(gtid)) {
            return true;
        }

        final GtidSet offsetGtidSet = new MariaDbGtidSet(gtid).retainAll(config.getGtidSourceFilter());
        final GtidSet earliestGtidSet = earliestAvailableGtidSet().retainAll(config.getGtidSourceFilter());
        return isAvailableFrom((MariaDbGtidSet) offsetGtidSet, (MariaDbGtidSet) earliestGtidSet);
    }

    /**
     * Get the GTID position at the start of the oldest binlog the server still has, read from the
     * {@code Gtid_list} event at the beginning of that binlog. Transactions up to and including these
     * positions are no longer available on the server.
     *
     * @return the GTID position at the start of the oldest binlog, empty if it cannot be determined
     */
    public MariaDbGtidSet earliestAvailableGtidSet() {
        try {
            final String binlogFileName = earliestBinlogFilename();
            if (binlogFileName == null) {
                return new MariaDbGtidSet("");
            }
            final String query = "SHOW BINLOG EVENTS IN '" + binlogFileName.replace("'", "''") + "' LIMIT 10";
            return queryAndMap(query, rs -> {
                while (rs.next()) {
                    if (GTID_LIST_EVENT_TYPE.equalsIgnoreCase(rs.getString("Event_type"))) {
                        final MariaDbGtidSet gtidSet = parseGtidListEventInfo(rs.getString("Info"));
                        LOGGER.info("Oldest available binlog '{}' starts at GTID position '{}'", binlogFileName, gtidSet);
                        return gtidSet;
                    }
                }
                LOGGER.warn("No {} event found at the start of binlog '{}'; cannot check whether required binlogs were purged",
                        GTID_LIST_EVENT_TYPE, binlogFileName);
                return new MariaDbGtidSet("");
            });
        }
        catch (SQLException | DebeziumException e) {
            LOGGER.warn("Failed to read the GTID position of the oldest binlog; cannot check whether required binlogs were purged", e);
            return new MariaDbGtidSet("");
        }
    }

    private static MariaDbGtidSet parseGtidListEventInfo(String info) {
        if (Strings.isNullOrBlank(info)) {
            return new MariaDbGtidSet("");
        }
        String value = info.trim();
        if (value.startsWith("[")) {
            value = value.substring(1);
        }
        if (value.endsWith("]")) {
            value = value.substring(0, value.length() - 1);
        }
        return new MariaDbGtidSet(value.replace(" ", ""));
    }

    private static boolean isAvailableFrom(MariaDbGtidSet offsetGtidSet, MariaDbGtidSet earliestGtidSet) {
        for (long domainId : offsetGtidSet.domainIds()) {
            final MariaDbGtidSet.MariaDbStreamSet earliest = earliestGtidSet.forDomain(domainId);
            if (earliest == null || earliest.isEmpty()) {
                continue;
            }
            final long offsetSequence = maxSequence(offsetGtidSet.forDomain(domainId));
            final long earliestSequence = maxSequence(earliest);
            if (offsetSequence < earliestSequence) {
                LOGGER.info("Connector requires GTID position {} for domain {}, but the oldest available binlog starts at {}",
                        offsetSequence, domainId, earliestSequence);
                return false;
            }
        }
        return true;
    }

    private static long maxSequence(MariaDbGtidSet.MariaDbStreamSet streamSet) {
        return streamSet.stream().mapToLong(MariaDbGtid::getSequence).max().orElse(0L);
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
}
