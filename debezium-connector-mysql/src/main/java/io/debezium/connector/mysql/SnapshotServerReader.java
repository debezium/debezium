/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.function.BufferedBlockingConsumer;
import io.debezium.function.Predicates;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

/**
 * A component that performs a snapshot of a MySQL server, and records the schema changes in {@link MySqlSchema}.
 * 
 * @author Randall Hauch
 */
public class SnapshotServerReader extends AbstractSnapshotReader {

    private final boolean includeData;
    private final SnapshotReaderMetrics metrics;
    private final SnapshotMySQLUtility snapshotMySQLUtility;

    /**
     * Create a snapshot reader.
     * 
     * @param name the name of this reader; may not be null
     * @param context the task context in which this reader is running; may not be null
     */
    public SnapshotServerReader(String name, MySqlTaskContext context) {
        super(name, context);
        this.includeData = !context.isSchemaOnlySnapshot();
        metrics = new SnapshotReaderMetrics(context.clock());
        metrics.register(context, logger);
        snapshotMySQLUtility = new SnapshotMySQLUtility(context);
    }

    /**
     * Perform the snapshot using the same logic as the "mysqldump" utility.
     */
    protected void execute() {
        context.configureLoggingContext("snapshot");
        final AtomicReference<String> sql = new AtomicReference<>();
        final JdbcConnection mysql = context.jdbc();
        final MySqlSchema schema = context.dbSchema();
        final SourceInfo source = context.source();
        final Clock clock = context.clock();
        final long ts = clock.currentTimeInMillis();
        logger.info("Starting snapshot for {} with user '{}'", context.connectionString(), mysql.username());
        logRolesForCurrentUser(mysql);
        logServerInformation(mysql);
        try {
            metrics.startSnapshot();

            snapshotMySQLUtility.beginTransaction();

            try {
                // I can't help but feel like there's got to be a better way to do this.
                if (!isRunning()) return;

                snapshotMySQLUtility.lockAllTables();

                // ------
                // START TRANSACTION
                // ------
                // First, start a transaction and request that a consistent MVCC snapshot is obtained immediately.
                // See http://dev.mysql.com/doc/refman/5.7/en/commit.html
                if (!isRunning()) return;
                logger.info("Step 2: start transaction with consistent snapshot");
                sql.set("START TRANSACTION WITH CONSISTENT SNAPSHOT");
                mysql.execute(sql.get());

                // ------------------------------------
                // READ BINLOG POSITION
                // ------------------------------------
                if (!isRunning()) return;
                //logger.info("\tsnapshot continuing with database(s): {}", includedDatabaseNames);

                snapshotMySQLUtility.maybeReadTableList();
                List<TableId> tableIds = snapshotMySQLUtility.getTableIds();
                Set<String> readableDatabaseNames = snapshotMySQLUtility.getReadableDatabaseNames();
                Map<String, List<TableId>> tableIdsByDbName = snapshotMySQLUtility.getTableIdsByDbName();


                // From this point forward, all source records produced by this connector will have an offset that includes a
                // "snapshot" field (with value of "true").

                // ------
                // STEP 6
                // ------
                // Transform the current schema so that it reflects the *current* state of the MySQL server's contents.
                // First, get the DROP TABLE and CREATE TABLE statement (with keys and constraint definitions) for our tables ...
                // Generate the DDL statements that set the charset-related system variables ...
                logger.info("Step {}: generating DROP and CREATE statements to reflect current database schemas:", 6);
                String setSystemVariablesStatement = context.setStatementFor(context.readMySqlCharsetSystemVariables(sql));
                schema.applyDdl(source, null, setSystemVariablesStatement, this::enqueueSchemaChanges);

                // Add DROP TABLE statements for all tables that we knew about AND those tables found in the databases ...
                Set<TableId> allTableIds = new HashSet<>(schema.tables().tableIds());
                allTableIds.addAll(tableIds);
                allTableIds.stream()
                           .filter(id -> isRunning()) // ignore all subsequent tables if this reader is stopped
                           .forEach(tableId -> schema.applyDdl(source, tableId.schema(),
                                                               "DROP TABLE IF EXISTS " + quote(tableId),
                                                               this::enqueueSchemaChanges));

                // Add a DROP DATABASE statement for each database that we no longer know about ...
                schema.tables().tableIds().stream().map(TableId::catalog)
                      .filter(Predicates.not(readableDatabaseNames::contains))
                      .filter(id -> isRunning()) // ignore all subsequent tables if this reader is stopped
                      .forEach(missingDbName -> schema.applyDdl(source, missingDbName,
                                                                "DROP DATABASE IF EXISTS " + quote(missingDbName),
                                                                this::enqueueSchemaChanges));

                // Now process all of our tables for each database ...
                for (Map.Entry<String, List<TableId>> entry : tableIdsByDbName.entrySet()) {
                    if (!isRunning()) break;
                    String dbName = entry.getKey();
                    // First drop, create, and then use the named database ...
                    schema.applyDdl(source, dbName, "DROP DATABASE IF EXISTS " + quote(dbName), this::enqueueSchemaChanges);
                    schema.applyDdl(source, dbName, "CREATE DATABASE " + quote(dbName), this::enqueueSchemaChanges);
                    schema.applyDdl(source, dbName, "USE " + quote(dbName), this::enqueueSchemaChanges);
                    for (TableId tableId : entry.getValue()) {
                        if (!isRunning()) break;
                        sql.set("SHOW CREATE TABLE " + quote(tableId));
                        mysql.query(sql.get(), rs -> {
                            if (rs.next()) {
                                schema.applyDdl(source, dbName, rs.getString(2), this::enqueueSchemaChanges);
                            }
                        });
                    }
                }
                context.makeRecord().regenerate();

                if (!isRunning()) return;
                if (includeData) {
                    BufferedBlockingConsumer<SourceRecord> bufferedRecordQueue = BufferedBlockingConsumer.bufferLast(super::enqueueRecord);
                    snapshotMySQLUtility.bufferLockedTables(recorder, bufferedRecordQueue);
                } else {
                    // source.markLastSnapshot(); Think we will not be needing this here it is used to mark last row entry?
                    logger.info("Step {}: encountered only schema based snapshot, skipping data snapshot", 7);
                }
            } finally {
                snapshotMySQLUtility.completeTransaction();
            }

            if (!isRunning()) {
                // The reader (and connector) was stopped and we did not finish ...
                try {
                    // Mark this reader as having completing its work ...
                    completeSuccessfully();
                    long stop = clock.currentTimeInMillis();
                    logger.info("Stopped snapshot after {} but before completing", Strings.duration(stop - ts));
                } finally {
                    // and since there's no more work to do clean up all resources ...
                    cleanupResources();
                }
            } else {
                // We completed the snapshot...
                try {
                    // Mark the source as having completed the snapshot. This will ensure the `source` field on records
                    // are not denoted as a snapshot ...
                    source.completeSnapshot();
                } finally {
                    // Set the completion flag ...
                    completeSuccessfully();
                    long stop = clock.currentTimeInMillis();
                    logger.info("Completed snapshot in {}", Strings.duration(stop - ts));
                }
            }
        } catch (Throwable e) {
            failed(e, "Aborting snapshot due to error when last running '" + sql.get() + "': " + e.getMessage());
        }
    }

    protected String quote(String dbOrTableName) {
        return "`" + dbOrTableName + "`";
    }

    protected String quote(TableId id) {
        return quote(id.catalog()) + "." + quote(id.table());
    }

    private void logServerInformation(JdbcConnection mysql) {
        try {
            logger.info("MySQL server variables related to change data capture:");
            mysql.query("SHOW VARIABLES WHERE Variable_name REGEXP 'version|binlog|tx_|gtid|character_set|collation|time_zone'", rs -> {
                while (rs.next()) {
                    logger.info("\t{} = {}",
                                Strings.pad(rs.getString(1), 45, ' '),
                                Strings.pad(rs.getString(2), 45, ' '));
                }
            });
        } catch (SQLException e) {
            logger.info("Cannot determine MySql server version", e);
        }
    }

    private void logRolesForCurrentUser(JdbcConnection mysql) {
        try {
            List<String> grants = new ArrayList<>();
            mysql.query("SHOW GRANTS FOR CURRENT_USER", rs -> {
                while (rs.next()) {
                    grants.add(rs.getString(1));
                }
            });
            if (grants.isEmpty()) {
                logger.warn("Snapshot is using user '{}' but it likely doesn't have proper privileges. " +
                        "If tables are missing or are empty, ensure connector is configured with the correct MySQL user " +
                        "and/or ensure that the MySQL user has the required privileges.",
                            mysql.username());
            } else {
                logger.info("Snapshot is using user '{}' with these MySQL grants:", mysql.username());
                grants.forEach(grant -> logger.info("\t{}", grant));
            }
        } catch (SQLException e) {
            logger.info("Cannot determine the privileges for '{}' ", mysql.username(), e);
        }
    }

    protected void enqueueSchemaChanges(String dbName, String ddlStatement) {
        if (!context.includeSchemaChangeRecords() || ddlStatement.length() == 0) {
            return;
        }
        if (context.makeRecord().schemaChanges(dbName, ddlStatement, super::enqueueRecord) > 0) {
            logger.info("\t{}", ddlStatement);
        }
    }
}
