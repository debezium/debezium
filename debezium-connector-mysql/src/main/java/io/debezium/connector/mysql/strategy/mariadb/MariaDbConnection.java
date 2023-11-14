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
                    LOGGER.info("knownGtidSet = {}", rs.getString(2));
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
        throw new DebeziumException("GtidSet subtraction not yet implemented by MariaDB");
    }

    @Override
    public GtidSet purgedGtidSet() {
        // MariaDB does not store purged GTID details in a variable like MySQL; however, it stores the
        // information in the `gtid_slave_pos` table in the `mysql` database, but this information has
        // slightly different semantics. The purging is handled by MariaDB through the binary log's
        // expiration settings and the `RESET MASTER` or `PURGE BINARY LOGS` statements.
        //
        // In order to calculate the purged state, we would need to get the `gtid_binlog_pos` variable
        // that shows the current position of the GTID in the binary log, used by the primary, and
        // compare this with the `gtid_slave_pos` variable on the replica server, which indicates the
        // position of the GTIDs that have been applied.
        throw new DebeziumException("Fetching purged GtidSet details is not yet supported");
    }

    @Override
    public GtidSet filterGtidSet(Predicate<String> gtidSourceFilter, String offsetGtids, GtidSet availableServerGtidSet, GtidSet purgedServerGtidSet) {
        throw new DebeziumException("NYI");
    }

    @Override
    protected GtidSet createGtidSet(String gtids) {
        return new MariaDbGtidSet(gtids);
    }
}
