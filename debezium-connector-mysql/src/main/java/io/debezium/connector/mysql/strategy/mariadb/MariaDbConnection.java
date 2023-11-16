/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.strategy.mariadb;

import java.sql.SQLException;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.mysql.GtidSet;
import io.debezium.connector.mysql.MySqlFieldReader;
import io.debezium.connector.mysql.strategy.AbstractConnectorConnection;
import io.debezium.connector.mysql.strategy.mariadb.MariaDbGtidSet.MariaDbGtid;

/**
 * An {@link AbstractConnectorConnection} for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbConnection extends AbstractConnectorConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(MariaDbConnection.class);

    public MariaDbConnection(MariaDbConnectionConfiguration connectionConfig, MySqlFieldReader fieldReader) {
        super(connectionConfig, fieldReader);
    }

    @Override
    public boolean isGtidModeEnabled() {
        // MariaDB always has GTID enabled; however, GTID_STRICT_MODE can be enabled or disabled.
        // For now we don't enforce this, so it can be a mixture
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
        // todo: have an open question to the MariaDB community on this to understand can this be deduced
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

    @Override
    public boolean isMariaDb() {
        return true;
    }

    @Override
    protected GtidSet createGtidSet(String gtids) {
        return new MariaDbGtidSet(gtids);
    }
}
