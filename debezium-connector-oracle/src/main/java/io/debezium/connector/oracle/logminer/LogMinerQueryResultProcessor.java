/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.jsqlparser.SimpleDmlParser;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.data.Envelope;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
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

    private final ChangeEventSource.ChangeEventSourceContext context;
    private final LogMinerMetrics metrics;
    private final TransactionalBuffer transactionalBuffer;
    private final SimpleDmlParser dmlParser;
    private final OracleOffsetContext offsetContext;
    private final OracleDatabaseSchema schema;
    private final EventDispatcher<TableId> dispatcher;
    private final TransactionalBufferMetrics transactionalBufferMetrics;
    private final String catalogName;
    private final Clock clock;
    private final Logger LOGGER = LoggerFactory.getLogger(LogMinerQueryResultProcessor.class);
    private long currentOffsetScn = 0;
    private long currentOffsetCommitScn = 0;
    private long stuckScnCounter = 0;

    LogMinerQueryResultProcessor(ChangeEventSource.ChangeEventSourceContext context, LogMinerMetrics metrics,
                                 TransactionalBuffer transactionalBuffer, SimpleDmlParser dmlParser,
                                 OracleOffsetContext offsetContext, OracleDatabaseSchema schema,
                                 EventDispatcher<TableId> dispatcher, TransactionalBufferMetrics transactionalBufferMetrics,
                                 String catalogName, Clock clock) {
        this.context = context;
        this.metrics = metrics;
        this.transactionalBuffer = transactionalBuffer;
        this.dmlParser = dmlParser;
        this.offsetContext = offsetContext;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.transactionalBufferMetrics = transactionalBufferMetrics;
        this.catalogName = catalogName;
        this.clock = clock;
    }

    /**
     * This method does all the job
     * @param resultSet the info from Log Miner view
     * @return number of processed DMLs from the given resultSet
     */
    int processResult(ResultSet resultSet) {
        int dmlCounter = 0;
        int commitCounter = 0;
        int rollbackCounter = 0;
        Duration cumulativeCommitTime = Duration.ZERO;
        Duration cumulativeParseTime = Duration.ZERO;
        Duration cumulativeOtherTime = Duration.ZERO;
        Instant startTime = Instant.now();
        while (true) {
            try {
                if (!resultSet.next()) {
                    break;
                }
            }
            catch (SQLException e) {
                LogMinerHelper.logError(transactionalBufferMetrics, "Closed resultSet");
                return 0;
            }

            Instant iterationStart = Instant.now();

            BigDecimal scn = RowMapper.getScn(transactionalBufferMetrics, resultSet);
            String redo_sql = RowMapper.getSqlRedo(transactionalBufferMetrics, resultSet);
            String tableName = RowMapper.getTableName(transactionalBufferMetrics, resultSet);
            String segOwner = RowMapper.getSegOwner(transactionalBufferMetrics, resultSet);
            int operationCode = RowMapper.getOperationCode(transactionalBufferMetrics, resultSet);
            Timestamp changeTime = RowMapper.getChangeTime(transactionalBufferMetrics, resultSet);
            String txId = RowMapper.getTransactionId(transactionalBufferMetrics, resultSet);
            String operation = RowMapper.getOperation(transactionalBufferMetrics, resultSet);
            String userName = RowMapper.getUsername(transactionalBufferMetrics, resultSet);

            LOGGER.trace("scn={}, operationCode={}, operation={}, table={}, segOwner={}, userName={}", scn, operationCode, operation, tableName, segOwner, userName);

            String logMessage = String.format("transactionId = %s, SCN= %s, table_name= %s, segOwner= %s, operationCode=%s, offsetSCN= %s, " +
                    " commitOffsetSCN= %s", txId, scn, tableName, segOwner, operationCode, offsetContext.getScn(), offsetContext.getCommitScn());

            if (scn == null) {
                LogMinerHelper.logWarn(transactionalBufferMetrics, "Scn is null for {}", logMessage);
                return 0;
            }

            // Commit
            if (operationCode == RowMapper.COMMIT) {
                if (transactionalBuffer.commit(txId, scn, offsetContext, changeTime, context, logMessage)) {
                    LOGGER.trace("COMMIT, {}", logMessage);
                    commitCounter++;
                    cumulativeCommitTime = cumulativeCommitTime.plus(Duration.between(iterationStart, Instant.now()));
                }
                continue;
            }

            // Rollback
            if (operationCode == RowMapper.ROLLBACK) {
                if (transactionalBuffer.rollback(txId, logMessage)) {
                    LOGGER.trace("ROLLBACK, {}", logMessage);
                    rollbackCounter++;
                }
                continue;
            }

            // DDL
            if (operationCode == RowMapper.DDL) {
                // todo: DDL operations are not yet supported during streaming while using Log Miner.
                LOGGER.info("DDL: {}, REDO_SQL: {}", logMessage, redo_sql);
                continue;
            }

            // MISSING_SCN
            if (operationCode == RowMapper.MISSING_SCN) {
                LogMinerHelper.logWarn(transactionalBufferMetrics, "Missing SCN,  {}", logMessage);
                continue;
            }

            // DML
            if (operationCode == RowMapper.INSERT || operationCode == RowMapper.DELETE || operationCode == RowMapper.UPDATE) {
                LOGGER.trace("DML,  {}, sql {}", logMessage, redo_sql);
                dmlCounter++;
                metrics.incrementCapturedDmlCount();
                iterationStart = Instant.now();
                LogMinerDmlEntry dmlEntry = dmlParser.parse(redo_sql, schema.getTables(), txId);
                cumulativeParseTime = cumulativeParseTime.plus(Duration.between(iterationStart, Instant.now()));
                iterationStart = Instant.now();

                if (dmlEntry == null || redo_sql == null) {
                    LOGGER.trace("Following statement was not parsed: {}, details: {}", redo_sql, logMessage);
                    continue;
                }

                // this will happen for instance on a excluded column change, we will omit this update
                if (dmlEntry.getCommandType().equals(Envelope.Operation.UPDATE)
                        && dmlEntry.getOldValues().size() == dmlEntry.getNewValues().size()
                        && dmlEntry.getNewValues().containsAll(dmlEntry.getOldValues())) {
                    LOGGER.trace("Following DML was skipped, " +
                            "most likely because of ignored excluded column change: {}, details: {}", redo_sql, logMessage);
                    continue;
                }

                dmlEntry.setObjectOwner(segOwner);
                dmlEntry.setSourceTime(changeTime);
                dmlEntry.setTransactionId(txId);
                dmlEntry.setObjectName(tableName);
                dmlEntry.setScn(scn);

                try {
                    TableId tableId = RowMapper.getTableId(catalogName, resultSet);
                    transactionalBuffer.registerCommitCallback(txId, scn, changeTime.toInstant(), redo_sql, (timestamp, smallestScn, commitScn, counter) -> {
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
                    cumulativeOtherTime = cumulativeOtherTime.plus(Duration.between(iterationStart, Instant.now()));

                }
                catch (Exception e) {
                    LogMinerHelper.logError(transactionalBufferMetrics, "Following dmlEntry: {} cannot be dispatched due to the : {}", dmlEntry, e);
                }
            }
        }
        metrics.setProcessedCapturedBatchDuration(Duration.between(startTime, Instant.now()));
        if (dmlCounter > 0 || commitCounter > 0 || rollbackCounter > 0) {
            warnStuckScn();
            currentOffsetScn = offsetContext.getScn();
            if (offsetContext.getCommitScn() != null) {
                currentOffsetCommitScn = offsetContext.getCommitScn();
            }
            LOGGER.debug("{} DMLs, {} Commits, {} Rollbacks. Millis - (total:{}, commit:{}, parse:{}, other:{}). " +
                    "Lag:{}. Offset scn:{}. Offset commit scn:{}. Active transactions:{}. Sleep time:{}",
                    dmlCounter, commitCounter, rollbackCounter, (Duration.between(startTime, Instant.now()).toMillis()),
                    cumulativeCommitTime.toMillis(), cumulativeParseTime.toMillis(), cumulativeOtherTime.toMillis(),
                    transactionalBufferMetrics.getLagFromSource(), offsetContext.getScn(), offsetContext.getCommitScn(),
                    transactionalBufferMetrics.getNumberOfActiveTransactions(), metrics.getMillisecondToSleepBetweenMiningQuery());
        }
        return dmlCounter;
    }

    /**
     * This method is warning if a long running transaction is discovered and could be abandoned in the future.
     * The criteria is the offset SCN remains the same in five mining cycles
     */
    private void warnStuckScn() {
        if (offsetContext != null && offsetContext.getCommitScn() != null) {
            if (currentOffsetScn == offsetContext.getScn() && currentOffsetCommitScn != offsetContext.getCommitScn()) {
                stuckScnCounter++;
                // logWarn only once
                if (stuckScnCounter == 5) {
                    LogMinerHelper.logWarn(transactionalBufferMetrics,
                            "Offset SCN {} did not change in five mining cycles, hence the oldest transaction was not committed. Offset commit SCN: {}", currentOffsetScn,
                            offsetContext.getCommitScn());
                    transactionalBufferMetrics.incrementScnFreezeCounter();
                }
            }
            else {
                stuckScnCounter = 0;
            }
        }
    }
}
