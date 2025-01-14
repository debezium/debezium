/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.nio.file.Path;

import org.apache.kafka.connect.source.SourceConnector;

import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.transforms.AbstractVectorToJsonConverterTest;

/**
 * @author Chris Cranford
 */
public abstract class BinlogVectorToJsonConverterIT<C extends SourceConnector>
        extends AbstractVectorToJsonConverterTest<C>
        implements BinlogConnectorTest<C> {

    protected static final String SERVER_NAME = "vector_to_json";
    protected static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-vt.txt").toAbsolutePath();

    protected final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase(SERVER_NAME, "vector_to_json").withDbHistoryPath(SCHEMA_HISTORY_PATH);

    protected void doBefore() throws Exception {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    protected void doAfter() throws Exception {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return getTestDatabaseConnection(DATABASE.getDatabaseName());
    }

}
