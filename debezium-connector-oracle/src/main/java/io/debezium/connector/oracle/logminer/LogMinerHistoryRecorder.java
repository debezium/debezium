/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.util.Threads;

/**
 * This class appends captured incremental changes into log miner history table.
 * Use cases:
 * 1. Troubleshooting/viewing history for auditing or debugging purposes
 * 2. Replaying recorded history
 *    - for regression testing. This will not cover all application part, like parsing, because we record already parsed redoSql.
 *      it could be beneficial for consumer testing for instance
 *    - if Kafka is down (further code modification needed)
 *    - for database migration. Do consistent dump using DPDUMP utility and replay the tail from this history table.
 * todo ensure thread safeness for multi-threaded parser branch
 * todo allow dynamic initialization after resolving DB problems
 */
@NotThreadSafe
public class LogMinerHistoryRecorder implements HistoryRecorder, AutoCloseable {

    private ExecutorService executor;
    private Supplier<Integer> recordQueueCapacity;

    private static int HISTORY_TABLE_SIZE_LIMIT = 1_000_000;

    private volatile PreparedStatement psInsert;
    private volatile CallableStatement bulkInsertCall;
    private volatile CallableStatement truncateTempTable;

    private OracleConnection recordHistoryConnection;
    private final LogMinerMetrics metrics;
    private JdbcConfiguration configuration;
    private long historyRetention;
    private int errorCounter = 0;

    private final static Logger LOGGER = LoggerFactory.getLogger(LogMinerHistoryRecorder.class);

    public LogMinerHistoryRecorder(LogMinerMetrics metrics, JdbcConfiguration configuration, long historyRetention) {
        this.metrics = metrics;
        this.configuration = configuration;
        this.historyRetention = historyRetention;

        try {
            createExecutor();
            connect();
            spawnHistoryTable();
            prepareStatements();
        }
        catch (SQLException e) {
            LOGGER.error("Cannot instantiate recorder due to the {}", e);
            close();
        }
    }

    /**
     * This method inserts one record into small temporary history table.
     * Each record has sequence field to preserve order of fetching from Log Miner view
     * GTT could be used here but the performance benefit is small, so using regular table
     * todo check if set "_optimizer_gather_stats_on_conventional_dml"=FALSE will eliminate occasional ORA-01438
     */
    @Override
    public void insertIntoTempTable(BigDecimal scn, String tableName, String segOwner, int operationCode, Timestamp changeTime,
                                    String transactionId, int csf, String redoSql) {
        if (metrics.getRecordMiningHistory() && executor != null) {
            executor.execute(() -> {
                try {
                    psInsert.setLong(1, scn.longValue());
                    psInsert.setString(2, tableName == null ? "" : tableName);
                    psInsert.setString(3, segOwner == null ? "" : segOwner);
                    psInsert.setLong(4, operationCode);
                    psInsert.setTimestamp(5, changeTime);
                    psInsert.setString(6, transactionId);
                    psInsert.setLong(7, csf);
                    psInsert.setString(8, redoSql);

                    psInsert.executeUpdate();

                    metrics.incrementTempHistoryTableRecordsCounter();
                    metrics.setRecordHistoryQueueCapacity(recordQueueCapacity.get());
                }
                catch (SQLException | RejectedExecutionException e) {
                    if (e instanceof SQLDataException) {
                        if (e.getMessage().contains("ORA-01438")) {
                            // retry one more time
                            try {
                                psInsert.executeUpdate();
                                metrics.incrementTempHistoryTableRecordsCounter();
                                metrics.setRecordHistoryQueueCapacity(recordQueueCapacity.get());
                                LOGGER.warn("Cannot insert data due to {}. " +
                                        "SCN: {}, Table name: {}, Segment Owner: {}, operation code: {}, ChangeTime: {}, transaction id: {}, csf: {}, sql: {}, retrying ...",
                                        e, scn.longValue(), tableName, segOwner, operationCode, changeTime, transactionId, csf, redoSql);
                                LOGGER.warn("Retry succeeded, ignore previous warning");
                                return;
                            }
                            catch (SQLException e1) {
                                LOGGER.error("Retry failed: {}", e1);
                            }
                        }
                    }
                    LOGGER.error("Cannot insert data due to {}. " +
                            "SCN: {}, Table name: {}, Segment Owner: {}, operation code: {}, ChangeTime: {}, transaction id: {}, csf: {}, sql: {}",
                            e, scn.longValue(), tableName, segOwner, operationCode, changeTime, transactionId, csf, redoSql);
                    // todo remove after figuring out what the problem is
                    if (errorCounter++ > 1000) {
                        close();
                        errorCounter = 0;
                    }
                }
            });
        }
    }

    /**
     * This method moves records from temporary table into large table in bulk, then truncates temp table.
     * This approach helps in possible performance degradation of inserting into a large table record by record.
     */
    @Override
    public void doBulkHistoryInsert() {
        if (metrics.getRecordMiningHistory() && executor != null) {
            executor.execute(() -> {
                Instant now = Instant.now();
                try {
                    if (metrics.getTempHistoryTableRecordsCounter() > 0) {

                        bulkInsertCall.execute();

                        truncateTempTable.execute();
                        recordHistoryConnection.commit();

                        metrics.incrementCurrentHistoryTableRecordsCounter();
                        metrics.resetTempHistoryTableRecordsCounter();
                        metrics.setRecordHistoryQueueCapacity(recordQueueCapacity.get());

                        if (metrics.getCurrentHistoryTableRecordsCounter() >= HISTORY_TABLE_SIZE_LIMIT) {
                            metrics.incrementTotalHistoryTableRecordsCounter();
                            metrics.resetCurrentHistoryTableRecordsCounter();
                            spawnHistoryTable();
                        }
                    }

                }
                catch (Throwable e) {
                    LOGGER.error("Cannot do bulk insert due to the {}", e);
                    if (SqlUtils.connectionProblem(e)) {
                        metrics.incrementNetworkConnectionProblemsCounter();

                        try {
                            connect();
                            spawnHistoryTable();
                            prepareStatements();
                        }
                        catch (SQLException sqlException) {
                            LOGGER.error("cannot reconnect the recorder {}, exiting...", sqlException);
                            close();
                        }
                    }
                }
                LOGGER.debug("time for bulk insert = {}", Duration.between(now, Instant.now()));
            });
        }
    }

    @Override
    public void close() {
        metrics.setRecordMiningHistory(false);
        closeExecutor();

        // Cleanup record history connection
        try {
            if (recordHistoryConnection.isConnected()) {
                if (psInsert != null && !psInsert.isClosed()) {
                    psInsert.close();
                    psInsert = null;
                }
                if (truncateTempTable != null && !truncateTempTable.isClosed()) {
                    truncateTempTable.close();
                    truncateTempTable = null;
                }
                if (bulkInsertCall != null && !bulkInsertCall.isClosed()) {
                    bulkInsertCall.close();
                    bulkInsertCall = null;
                }
                recordHistoryConnection.close();
                recordHistoryConnection = null;
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to close record history connection successfully", e);
        }
    }

    private void connect() throws SQLException {
        if (recordHistoryConnection != null) {
            try {
                recordHistoryConnection.close();
            }
            catch (SQLException e) {
                LOGGER.error("Cannot close resource {}, ", e);
            }
        }
        recordHistoryConnection = new OracleConnection(configuration, () -> getClass().getClassLoader());
        recordHistoryConnection.connect().setAutoCommit(false);
    }

    private void prepareStatements() throws SQLException {
        if (psInsert != null) {
            try {
                psInsert.close();
            }
            catch (SQLException e) {
                LOGGER.error("Cannot close resource {}, ", e);
            }
        }

        if (truncateTempTable != null) {
            try {
                truncateTempTable.close();
            }
            catch (SQLException e) {
                LOGGER.error("Cannot close resource {}, ", e);
            }
        }

        psInsert = recordHistoryConnection.connection().prepareStatement(SqlUtils.INSERT_INTO_TEMP_HISTORY_TABLE_STMT);
        truncateTempTable = recordHistoryConnection.connection().prepareCall(SqlUtils.truncateTableStatement(SqlUtils.LOGMNR_HISTORY_TEMP_TABLE));
    }

    private void spawnHistoryTable() throws SQLException {
        String currentHistoryTableName = SqlUtils.buildHistoryTableName(LocalDateTime.now());
        LogMinerHelper.createLogMiningHistoryObjects(recordHistoryConnection.connection(), currentHistoryTableName);
        if (bulkInsertCall != null) {
            try {
                bulkInsertCall.close();
            }
            catch (SQLException e) {
                LOGGER.error("Cannot close resource {}, ", e);
            }
        }
        bulkInsertCall = recordHistoryConnection.connection().prepareCall(SqlUtils.bulkHistoryInsertStmt(currentHistoryTableName));
        LogMinerHelper.deleteOutdatedHistory(recordHistoryConnection.connection(), historyRetention);
    }

    private void createExecutor() {
        final BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(this.metrics.getMiningHistoryQueueLimit());
        executor = new ThreadPoolExecutor(1, 1,
                Integer.MAX_VALUE, TimeUnit.MILLISECONDS,
                workQueue,
                Threads.threadFactory(OracleConnector.class, "log-miner", "history-recorder", true, false),
                new ThreadPoolExecutor.CallerRunsPolicy());
        recordQueueCapacity = workQueue::remainingCapacity;
    }

    private void closeExecutor() {
        if (executor != null) {
            executor.shutdown();
            try {
                boolean terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
                if (!terminated) {
                    executor.shutdownNow();
                }
            }
            catch (InterruptedException e) {
                LOGGER.warn("The executor was not terminated properly");
            }
        }
    }
}
