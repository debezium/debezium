/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.sql.SQLException;

import org.apache.kafka.connect.source.SourceConnector;

import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;

/**
 * Abstract base class for all binlog-based connector integration tests.
 *
 * @author Chris Cranford
 */
public abstract class AbstractBinlogConnectorIT<C extends SourceConnector>
        extends AbstractAsyncEngineConnectorTest
        implements BinlogConnectorTest<C> {

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

}
