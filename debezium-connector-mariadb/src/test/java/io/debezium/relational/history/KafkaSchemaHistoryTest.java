/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import io.debezium.config.Configuration;
import io.debezium.connector.mariadb.MariaDbConnectorConfig;
import io.debezium.connector.mariadb.MariaDbOffsetContext;
import io.debezium.connector.mariadb.MariaDbPartition;
import io.debezium.connector.mariadb.MariaDbReadOnlyIncrementalSnapshotContext;
import io.debezium.connector.mariadb.SourceInfo;
import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.ddl.DdlParser;

/**
 * @author Chris Cranford
 */
public class KafkaSchemaHistoryTest extends AbstractKafkaSchemaHistoryTest<MariaDbPartition, MariaDbOffsetContext> {
    @Override
    protected MariaDbPartition createPartition(String serverName, String databaseName) {
        return new MariaDbPartition(serverName, databaseName);
    }

    @Override
    protected MariaDbOffsetContext createOffsetContext(Configuration config) {
        return new MariaDbOffsetContext(
                null,
                true,
                new TransactionContext(),
                new MariaDbReadOnlyIncrementalSnapshotContext<>(),
                new SourceInfo(new MariaDbConnectorConfig(config)));
    }

    @Override
    protected DdlParser getDdlParser() {
        return new MariaDbAntlrDdlParser();
    }
}
