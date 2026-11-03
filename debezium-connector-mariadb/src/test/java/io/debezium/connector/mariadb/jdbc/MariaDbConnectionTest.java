/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.connector.mariadb.MariaDbConnectorConfig;
import io.debezium.connector.mariadb.gtid.MariaDbGtidSet;
import io.debezium.doc.FixFor;

/**
 * Verifies how the MariaDB connection decides whether the GTID position stored in the offsets can still be
 * streamed from the binlogs that the server has kept.
 */
public class MariaDbConnectionTest {

    @Test
    @FixFor("debezium/dbz#1384")
    void reportsPositionUnavailableWhenNeededBinlogsWerePurged() {
        // The server is at 0-1-50000, but its oldest binlog starts after 0-1-40000, so 0-1-100 is gone
        final ServerState connection = new ServerState(config(), "0-1-50000", "0-1-40000");
        assertThat(connection.isBinlogPositionAvailable(connection.connectorConfig, "0-1-100", null)).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1384")
    void reportsPositionAvailableWhenNeededBinlogsAreKept() {
        final ServerState connection = new ServerState(config(), "0-1-50000", "0-1-40000");
        assertThat(connection.isBinlogPositionAvailable(connection.connectorConfig, "0-1-40000", null)).isTrue();
        assertThat(connection.isBinlogPositionAvailable(connection.connectorConfig, "0-1-45000", null)).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1384")
    void reportsPositionAvailableWhenEarliestBinlogPositionIsUnknown() {
        final ServerState connection = new ServerState(config(), "0-1-50000", "");
        assertThat(connection.isBinlogPositionAvailable(connection.connectorConfig, "0-1-100", null)).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1384")
    void comparesOffsetWithEarliestAvailablePositionPerDomain() {
        final ServerState connection = new ServerState(config(), "0-1-50000,1-2-50000,5-1-50000", "0-1-100,1-2-7");

        // Below the oldest binlog in one domain
        assertThat(connection.isBinlogPositionAvailable(connection.connectorConfig, "0-1-99", null)).isFalse();
        assertThat(connection.isBinlogPositionAvailable(connection.connectorConfig, "0-1-500,1-2-6", null)).isFalse();

        // Equal means the next transaction is still in the oldest binlog
        assertThat(connection.isBinlogPositionAvailable(connection.connectorConfig, "0-1-100,1-2-7", null)).isTrue();
        assertThat(connection.isBinlogPositionAvailable(connection.connectorConfig, "0-1-101,1-2-8", null)).isTrue();

        // Server id is not part of the comparison, only the domain
        assertThat(connection.isBinlogPositionAvailable(connection.connectorConfig, "0-3-150", null)).isTrue();
        assertThat(connection.isBinlogPositionAvailable(connection.connectorConfig, "0-3-50", null)).isFalse();

        // A domain the oldest binlog does not know about is not treated as purged
        assertThat(connection.isBinlogPositionAvailable(connection.connectorConfig, "5-1-1", null)).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1384")
    void ignoresDomainsExcludedByGtidSourceFilter() {
        final MariaDbConnectorConfig config = new MariaDbConnectorConfig(baseConfig()
                .with(MariaDbConnectorConfig.GTID_SOURCE_EXCLUDES, "1-2")
                .build());
        final ServerState connection = new ServerState(config, "0-1-500,1-2-50", "0-1-100,1-2-40");
        assertThat(connection.isBinlogPositionAvailable(config, "0-1-200,1-2-10", null)).isTrue();
    }

    private static MariaDbConnectorConfig config() {
        return new MariaDbConnectorConfig(baseConfig().build());
    }

    private static Configuration.Builder baseConfig() {
        return Configuration.create()
                .with(MariaDbConnectorConfig.HOSTNAME, "localhost")
                .with(MariaDbConnectorConfig.PORT, 3306)
                .with(MariaDbConnectorConfig.USER, "mariadbuser")
                .with(MariaDbConnectorConfig.PASSWORD, "mariadbpw")
                .with(MariaDbConnectorConfig.SERVER_ID, 18765)
                .with(MariaDbConnectorConfig.TOPIC_PREFIX, "mariadb-server");
    }

    /**
     * A connection that answers the two server state queries with fixed values instead of asking a database.
     */
    private static class ServerState extends MariaDbConnection {

        private final MariaDbConnectorConfig connectorConfig;
        private final String currentPosition;
        private final String earliestPosition;

        ServerState(MariaDbConnectorConfig connectorConfig, String currentPosition, String earliestPosition) {
            super(new MariaDbConnectionConfiguration(connectorConfig.getConfig()), new MariaDbFieldReader(connectorConfig));
            this.connectorConfig = connectorConfig;
            this.currentPosition = currentPosition;
            this.earliestPosition = earliestPosition;
        }

        @Override
        public GtidSet knownGtidSet() {
            return new MariaDbGtidSet(currentPosition);
        }

        @Override
        public MariaDbGtidSet earliestAvailableGtidSet() {
            return new MariaDbGtidSet(earliestPosition);
        }
    }
}
