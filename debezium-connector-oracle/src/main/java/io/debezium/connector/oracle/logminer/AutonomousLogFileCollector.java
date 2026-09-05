/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.sql.SQLException;
import java.time.Duration;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;

/**
 * A collector for Oracle Autonomous AI Database that is responsible for fetching, deduplication, and supplying Debezium
 * with a set of {@link LogFile} instances that should be mined given a specific {@link Scn}.
 *
 * @author Gab David
 */
public class AutonomousLogFileCollector extends LogFileCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(AutonomousLogFileCollector.class);

    private final Duration archiveLogRetention;
    protected final OracleConnection connection;

    public AutonomousLogFileCollector(OracleConnectorConfig connectorConfig, OracleConnection connection) {
        super(connectorConfig, connection);
        this.connection = connection;
        this.archiveLogRetention = connectorConfig.getArchiveLogRetention();
    }

    @Override
    public LogFileCollector.LogFilesResult getLogs(Scn offsetScn) throws SQLException, LogFileNotFoundException {
        LOGGER.debug("Collecting logs based on the read SCN position {}.", offsetScn);
        final List<LogFile> files = getLogsForOffsetScn(offsetScn);
        return new LogFilesResult(files, connection.getRedoThreadState());
    }

    @Override
    @VisibleForTesting
    public List<LogFile> getLogsForOffsetScn(Scn offsetScn) throws SQLException {
        return connection.queryAndMap(getLogsQuery(offsetScn), rs -> {
            final Map<String, List<LogFile>> archiveLogsByDestination = new HashMap<>();

            while (rs.next()) {
                final LogFile logFile = createLogFileFromResultSetRow(rs);

                // Index 12 here is DEST_ID instead of DEST_NAME because Autonomous DB disables the
                // V$ARCHIVE_DEST_STATUS view so we can't join on it to get the DEST_NAME. Luckily DEST_NAME
                // is only used for deduping with the redo list which we don't need to do.
                final String destinationId = rs.getString(12);
                if (logFile.isArchive() && logFile.getNextScn().compareTo(offsetScn) >= 0) {
                    LOGGER.debug(
                            "Archive log {} Seq# {} Thread# {} SCN [{} - {} (delta {})] Size {} bytes Dictionary {}/{} in destination {} to be added.",
                            logFile.getFileName(),
                            logFile.getSequence(),
                            logFile.getThread(),
                            logFile.getFirstScn(),
                            logFile.getNextScn(),
                            logFile.getNextScn().subtract(logFile.getFirstScn()),
                            String.format("%,d", logFile.getBytes()),
                            logFile.hasDictionaryStart() ? "Y" : "N",
                            logFile.hasDictionaryEnd() ? "Y" : "N",
                            destinationId);

                    archiveLogsByDestination.computeIfAbsent(destinationId, k -> new ArrayList<>()).add(logFile);
                }
                else if (logFile.isRedo()) {
                    LOGGER.debug("Redo log {} found but is not supported.", logFile.getFileName());
                }
            }

            return mergeLogs(archiveLogsByDestination);
        });
    }

    public static List<LogFile> mergeLogs(Map<String, List<LogFile>> logs) {
        final List<LogFile> result = new ArrayList<>();
        final Set<LogFile.ThreadSequence> seen = new HashSet<>();

        for (List<LogFile> destinationLogs : logs.values()) {
            for (LogFile logFile : destinationLogs) {
                if (seen.add(logFile.getThreadSequence())) {
                    result.add(logFile);
                }
            }
        }

        return result;
    }

    private String getLogsQuery(Scn offsetScn) {
        // Autonomous DB auto manages log files and only supports archive.log.only.mode.
        return SqlUtils.allMinableLogsQuery(offsetScn, archiveLogRetention, true, Collections.emptyList(), true);
    }
}
