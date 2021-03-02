/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningDmlParser;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.logminer.parser.DmlParser;
import io.debezium.connector.oracle.logminer.parser.DmlParserException;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlParser;
import io.debezium.connector.oracle.logminer.parser.SimpleDmlParser;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * This class process entries obtained from LogMiner view.
 * It parses each entry.
 * On each DML it registers a callback in TransactionalBuffer.
 * On rollback it removes registered entries from TransactionalBuffer.
 * On commit it executes all registered callbacks, which dispatch ChangeRecords.
 * This also calculates metrics
 */
class LogMinerQueryResultProcessor {

    private final ChangeEventSourceContext context;
    private final LogMinerMetrics metrics;
    private final TransactionalBuffer transactionalBuffer;
    private final DmlParser dmlParser;
    private final OracleOffsetContext offsetContext;
    private final OracleDatabaseSchema schema;
    private final EventDispatcher<TableId> dispatcher;
    private final TransactionalBufferMetrics transactionalBufferMetrics;
    private final OracleConnectorConfig connectorConfig;
    private final Clock clock;
    private final Logger LOGGER = LoggerFactory.getLogger(LogMinerQueryResultProcessor.class);
    private long currentOffsetScn = 0;
    private long currentOffsetCommitScn = 0;
    private long stuckScnCounter = 0;
    private HistoryRecorder historyRecorder;

    LogMinerQueryResultProcessor(ChangeEventSourceContext context, OracleConnection jdbcConnection,
                                 OracleConnectorConfig connectorConfig, LogMinerMetrics metrics,
                                 TransactionalBuffer transactionalBuffer,
                                 OracleOffsetContext offsetContext, OracleDatabaseSchema schema,
                                 EventDispatcher<TableId> dispatcher,
                                 Clock clock, HistoryRecorder historyRecorder) {
        this.context = context;
        this.metrics = metrics;
        this.transactionalBuffer = transactionalBuffer;
        this.offsetContext = offsetContext;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.transactionalBufferMetrics = transactionalBuffer.getMetrics();
        this.clock = clock;
        this.historyRecorder = historyRecorder;
        this.connectorConfig = connectorConfig;
        this.dmlParser = resolveParser(connectorConfig, jdbcConnection);
    }

    private static DmlParser resolveParser(OracleConnectorConfig connectorConfig, OracleConnection connection) {
        if (connectorConfig.getLogMiningDmlParser().equals(LogMiningDmlParser.LEGACY)) {
            OracleValueConverters converter = new OracleValueConverters(connectorConfig, connection);
            return new SimpleDmlParser(connectorConfig.getCatalogName(), converter);
        }
        return new LogMinerDmlParser();
    }

    /**
     * This method does all the job
     * @param resultSet the info from LogMiner view
     * @return number of processed DMLs from the given resultSet
     */
    int processResult(ResultSet resultSet) {
        int dmlCounter = 0, insertCounter = 0, updateCounter = 0, deleteCounter = 0;
        int commitCounter = 0;
        int rollbackCounter = 0;
        long rows = 0;
        Instant startTime = Instant.now();
        while (context.isRunning()) {
            try {
                Instant rsNextStart = Instant.now();
                if (!resultSet.next()) {
                    break;
                }
                rows++;
                metrics.addCurrentResultSetNext(Duration.between(rsNextStart, Instant.now()));
            }
            catch (SQLException e) {
                LogMinerHelper.logError(transactionalBufferMetrics, "Closed resultSet");
                return 0;
            }

            Scn scn = RowMapper.getScn(transactionalBufferMetrics, resultSet);
            String tableName = RowMapper.getTableName(transactionalBufferMetrics, resultSet);
            String segOwner = RowMapper.getSegOwner(transactionalBufferMetrics, resultSet);
            int operationCode = RowMapper.getOperationCode(transactionalBufferMetrics, resultSet);
            Timestamp changeTime = RowMapper.getChangeTime(transactionalBufferMetrics, resultSet);
            String txId = RowMapper.getTransactionId(transactionalBufferMetrics, resultSet);
            String operation = RowMapper.getOperation(transactionalBufferMetrics, resultSet);
            String userName = RowMapper.getUsername(transactionalBufferMetrics, resultSet);
            boolean isDml = false;
            if (operationCode == RowMapper.INSERT || operationCode == RowMapper.UPDATE || operationCode == RowMapper.DELETE) {
                isDml = true;
            }
            String redoSql = RowMapper.getSqlRedo(transactionalBufferMetrics, resultSet, isDml, historyRecorder, scn, tableName, segOwner, operationCode, changeTime,
                    txId);

            LOGGER.trace("scn={}, operationCode={}, operation={}, table={}, segOwner={}, userName={}", scn, operationCode, operation, tableName, segOwner, userName);

            String logMessage = String.format("transactionId=%s, SCN=%s, table_name=%s, segOwner=%s, operationCode=%s, offsetSCN=%s, " +
                    " commitOffsetSCN=%s", txId, scn, tableName, segOwner, operationCode, offsetContext.getScn(), offsetContext.getCommitScn());

            if (scn == null) {
                LogMinerHelper.logWarn(transactionalBufferMetrics, "Scn is null for {}", logMessage);
                return 0;
            }

            // Commit
            if (operationCode == RowMapper.COMMIT) {
                if (transactionalBuffer.isTransactionRegistered(txId)) {
                    historyRecorder.record(scn, tableName, segOwner, operationCode, changeTime, txId, 0, redoSql);
                }
                if (transactionalBuffer.commit(txId, scn, offsetContext, changeTime, context, logMessage, dispatcher)) {
                    LOGGER.trace("COMMIT, {}", logMessage);
                    commitCounter++;
                }
                continue;
            }

            // Rollback
            if (operationCode == RowMapper.ROLLBACK) {
                if (transactionalBuffer.isTransactionRegistered(txId)) {
                    historyRecorder.record(scn, tableName, segOwner, operationCode, changeTime, txId, 0, redoSql);
                }
                if (transactionalBuffer.rollback(txId, logMessage)) {
                    LOGGER.trace("ROLLBACK, {}", logMessage);
                    rollbackCounter++;
                }
                continue;
            }

            // DDL
            if (operationCode == RowMapper.DDL) {
                // todo: DDL operations are not yet supported during streaming while using LogMiner.
                historyRecorder.record(scn, tableName, segOwner, operationCode, changeTime, txId, 0, redoSql);
                LOGGER.info("DDL: {}, REDO_SQL: {}", logMessage, redoSql);
                continue;
            }

            // MISSING_SCN
            if (operationCode == RowMapper.MISSING_SCN) {
                historyRecorder.record(scn, tableName, segOwner, operationCode, changeTime, txId, 0, redoSql);
                LogMinerHelper.logWarn(transactionalBufferMetrics, "Missing SCN,  {}", logMessage);
                continue;
            }

            // DML
            if (operationCode == RowMapper.INSERT || operationCode == RowMapper.DELETE || operationCode == RowMapper.UPDATE) {
                final TableId tableId = RowMapper.getTableId(connectorConfig.getCatalogName(), resultSet);

                LOGGER.trace("DML,  {}, sql {}", logMessage, redoSql);
                if (redoSql == null) {
                    LOGGER.trace("Redo SQL was empty, DML operation skipped.");
                    continue;
                }

                dmlCounter++;
                switch (operationCode) {
                    case RowMapper.INSERT:
                        insertCounter++;
                        break;
                    case RowMapper.UPDATE:
                        updateCounter++;
                        break;
                    case RowMapper.DELETE:
                        deleteCounter++;
                        break;
                }

                final LogMinerDmlEntry dmlEntry = parse(redoSql, schema, tableId, txId);
                dmlEntry.setObjectOwner(segOwner);
                dmlEntry.setSourceTime(changeTime);
                dmlEntry.setTransactionId(txId);
                dmlEntry.setObjectName(tableName);
                dmlEntry.setScn(scn);

                try {
                    transactionalBuffer.registerCommitCallback(txId, scn, changeTime.toInstant(), (timestamp, smallestScn, commitScn, counter) -> {
                        // update SCN in offset context only if processed SCN less than SCN among other transactions
                        if (smallestScn == null || scn.compareTo(smallestScn) < 0) {
                            offsetContext.setScn(scn.longValue());
                            transactionalBufferMetrics.setOldestScn(scn.longValue());
                        }
                        offsetContext.setTransactionId(txId);
                        offsetContext.setSourceTime(timestamp.toInstant());
                        offsetContext.setTableId(tableId);
                        if (counter == 0) {
                            offsetContext.setCommitScn(commitScn.longValue());
                        }
                        Table table = schema.tableFor(tableId);
                        LOGGER.trace("Processing DML event {} scn {}", dmlEntry.toString(), scn);

                        dispatcher.dispatchDataChangeEvent(tableId,
                                new LogMinerChangeRecordEmitter(offsetContext, dmlEntry, table, clock));
                    });

                }
                catch (Exception e) {
                    LogMinerHelper.logError(transactionalBufferMetrics, "Following dmlEntry: {} cannot be dispatched due to the : {}", dmlEntry, e);
                }
            }
        }

        Duration totalTime = Duration.between(startTime, Instant.now());
        if (dmlCounter > 0 || commitCounter > 0 || rollbackCounter > 0) {
            metrics.setLastCapturedDmlCount(dmlCounter);
            metrics.setLastDurationOfBatchProcessing(totalTime);

            warnStuckScn();
            currentOffsetScn = offsetContext.getScn();
            if (offsetContext.getCommitScn() != null) {
                currentOffsetCommitScn = offsetContext.getCommitScn();
            }
        }

        LOGGER.debug("{} Rows, {} DMLs, {} Commits, {} Rollbacks, {} Inserts, {} Updates, {} Deletes. Processed in {} millis. " +
                "Lag:{}. Offset scn:{}. Offset commit scn:{}. Active transactions:{}. Sleep time:{}",
                rows, dmlCounter, commitCounter, rollbackCounter, insertCounter, updateCounter, deleteCounter, totalTime.toMillis(),
                transactionalBufferMetrics.getLagFromSource(), offsetContext.getScn(), offsetContext.getCommitScn(),
                transactionalBufferMetrics.getNumberOfActiveTransactions(), metrics.getMillisecondToSleepBetweenMiningQuery());

        metrics.addProcessedRows(rows);
        historyRecorder.flush();
        return dmlCounter;
    }

    /**
     * This method is warning if a long running transaction is discovered and could be abandoned in the future.
     * The criteria is the offset SCN remains the same in 25 mining cycles
     */
    private void warnStuckScn() {
        if (offsetContext != null && offsetContext.getCommitScn() != null) {
            if (currentOffsetScn == offsetContext.getScn() && currentOffsetCommitScn != offsetContext.getCommitScn()) {
                stuckScnCounter++;
                // logWarn only once
                if (stuckScnCounter == 25) {
                    LogMinerHelper.logWarn(transactionalBufferMetrics,
                            "Offset SCN {} is not changing. It indicates long transaction(s). " +
                                    "Offset commit SCN: {}",
                            currentOffsetScn, offsetContext.getCommitScn());
                    transactionalBufferMetrics.incrementScnFreezeCounter();
                }
            }
            else {
                stuckScnCounter = 0;
            }
        }
    }

    private LogMinerDmlEntry parse(String redoSql, OracleDatabaseSchema schema, TableId tableId, String txId) {
        LogMinerDmlEntry dmlEntry;
        try {
            Instant parseStart = Instant.now();
            dmlEntry = dmlParser.parse(redoSql, schema.getTables(), tableId, txId);
            metrics.addCurrentParseTime(Duration.between(parseStart, Instant.now()));
        }
        catch (DmlParserException e) {
            StringBuilder message = new StringBuilder();
            message.append("DML statement couldn't be parsed.");
            message.append(" Please open a Jira issue with the statement '").append(redoSql).append("'.");
            if (LogMiningDmlParser.FAST.equals(connectorConfig.getLogMiningDmlParser())) {
                message.append(" You can set internal.log.mining.dml.parser='legacy' as a workaround until the parse error is fixed.");
            }
            throw new DmlParserException(message.toString(), e);
        }
        return dmlEntry;
    }
}
