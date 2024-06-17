/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogDatabaseSchemaTest;
import io.debezium.connector.binlog.charset.BinlogCharsetRegistry;
import io.debezium.connector.mariadb.jdbc.MariaDbValueConverters;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * @author Chris Cranford
 */
public class DatabaseSchemaTest extends BinlogDatabaseSchemaTest<MariaDbConnectorConfig, MariaDbDatabaseSchema, MariaDbPartition, MariaDbOffsetContext> {
    @Override
    protected MariaDbConnectorConfig getConnectorConfig(Configuration config) {
        config = config.edit().with(AbstractSchemaHistory.INTERNAL_PREFER_DDL, true).build();
        return new MariaDbConnectorConfig(config);
    }

    @Override
    protected MariaDbDatabaseSchema getSchema(Configuration config) {
        this.connectorConfig = getConnectorConfig(config);

        final MariaDbValueConverters valueConverters = new MariaDbValueConverters(
                JdbcValueConverters.DecimalMode.PRECISE,
                TemporalPrecisionMode.ADAPTIVE,
                JdbcValueConverters.BigIntUnsignedMode.LONG,
                CommonConnectorConfig.BinaryHandlingMode.BYTES,
                MariaDbValueConverters::adjustTemporal,
                CommonConnectorConfig.EventConvertingFailureHandlingMode.WARN,
                connectorConfig.getServiceRegistry().getService(BinlogCharsetRegistry.class));

        return new MariaDbDatabaseSchema(
                connectorConfig,
                valueConverters,
                (TopicNamingStrategy) DefaultTopicNamingStrategy.create(connectorConfig),
                SchemaNameAdjuster.create(),
                false);
    }

    @Override
    protected MariaDbPartition initializePartition(MariaDbConnectorConfig connectorConfig, Configuration taskConfig) {
        Set<MariaDbPartition> partitions = new MariaDbPartition.Provider(connectorConfig, taskConfig).getPartitions();
        assertThat(partitions).hasSize(1);
        return partitions.iterator().next();
    }

    @Override
    protected MariaDbOffsetContext initializeOffset(MariaDbConnectorConfig connectorConfig) {
        return MariaDbOffsetContext.initial(connectorConfig);
    }
}
