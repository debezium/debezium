/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import io.confluent.credentialproviders.DefaultJdbcCredentialsProvider;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.connector.mysql.MySqlReadOnlyIncrementalSnapshotContext;
import io.debezium.connector.mysql.SourceInfo;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.ddl.DdlParser;

/**
 * @author Randall Hauch
 */
public class KafkaSchemaHistoryTest extends AbstractKafkaSchemaHistoryTest<MySqlPartition, MySqlOffsetContext> {
    @Override
    protected MySqlPartition createPartition(String serverName, String databaseName) {
        return new MySqlPartition(serverName, databaseName);
    }

    @Override
    protected MySqlOffsetContext createOffsetContext(Configuration config) {
        return new MySqlOffsetContext(
                null,
                true,
                new TransactionContext(),
                new MySqlReadOnlyIncrementalSnapshotContext<>(),
                new SourceInfo(new MySqlConnectorConfig(config, new DefaultJdbcCredentialsProvider())));
    }

    @Override
    protected DdlParser getDdlParser() {
        return new MySqlAntlrDdlParser();
    }
}
