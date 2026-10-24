/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.data.Envelope;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.JdbcConnection;

/**
 * Abstract base class for all binlog-based connector integration tests.
 *
 * @author Chris Cranford
 */
public abstract class AbstractBinlogConnectorIT<C extends SourceConnector>
        extends AbstractAsyncEngineConnectorTest
        implements BinlogConnectorTest<C> {

    /**
     * Initial snapshots use DDL schemas; streaming schemas reconstructed from TABLE_MAP have no defaults.
     * Row values are asserted separately against the values materialized by the database in both modes.
     */
    protected <T> T expectedSchemaDefault(SourceRecord record, T ddlDefault) {
        final var envelope = (Struct) record.value();
        if (config.getBoolean(BinlogConnectorConfig.BINLOG_METADATA_BASED_SCHEMA)
                && !Envelope.Operation.READ.code().equals(envelope.getString(Envelope.FieldName.OPERATION))) {
            return null;
        }
        return ddlDefault;
    }

    // todo: find a better way to refactor this
    @Override
    public boolean isMariaDb() {
        try (BinlogTestConnection connection = getTestDatabaseConnection("mysql")) {
            return connection.isMariaDb();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isMySQL5() {
        try (BinlogTestConnection connection = getTestDatabaseConnection("mysql")) {
            return connection.isMySQL5();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isPerconaServer() {
        try (BinlogTestConnection connection = getTestDatabaseConnection("mysql")) {
            return connection.isPercona();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void executeStatements(String targetDatabase, String... statements) {
        try (JdbcConnection jdbcConnection = getTestDatabaseConnection(targetDatabase).connect();
                var statement = jdbcConnection.connection().createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExecutorService executeScheduledStatement(int times,
                                                     long period,
                                                     int delay,
                                                     String databaseName,
                                                     String statement)
            throws SQLException {

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        JdbcConnection jdbcConnection = getTestDatabaseConnection(databaseName).connect();
        CountDownLatch latch = new CountDownLatch(times);

        executorService.scheduleAtFixedRate(
                () -> {
                    try {
                        jdbcConnection.execute(String.format(statement, latch.getCount()));
                        latch.countDown();
                        if (latch.getCount() == 0) {
                            jdbcConnection.close();
                        }
                    }
                    catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }, delay, period, TimeUnit.MILLISECONDS);

        return executorService;
    }
}
