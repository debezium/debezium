/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.List;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogIncrementalSnapshotRecoveryIT;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.mysql.jdbc.MySqlConnection;
import io.debezium.connector.mysql.jdbc.MySqlConnectionConfiguration;
import io.debezium.connector.mysql.jdbc.MySqlFieldReaderResolver;
import io.debezium.connector.mysql.jdbc.MySqlValueConverters;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

class IncrementalSnapshotRecoveryIT
        extends BinlogIncrementalSnapshotRecoveryIT<MySqlConnector, MySqlPartition, MySqlOffsetContext>
        implements MySqlCommon {

    @Override
    protected MySqlConnectorConfig createConfig(Configuration configuration) {
        return new MySqlConnectorConfig(configuration);
    }

    @Override
    protected BinlogConnectorConnection createConnection(Configuration configuration) {
        return new MySqlConnection(new MySqlConnectionConfiguration(configuration),
                MySqlFieldReaderResolver.resolve((MySqlConnectorConfig) config));
    }

    @Override
    protected MySqlTaskContext createTaskContext(Configuration configuration) {
        return new MySqlTaskContext(configuration, (MySqlConnectorConfig) config);
    }

    @Override
    protected MySqlDatabaseSchema createSchema(CdcSourceTaskContext<?> taskContext) {
        return new MySqlDatabaseSchema((MySqlConnectorConfig) config,
                new MySqlValueConverters(config.getDecimalMode(), config.getTemporalPrecisionMode(),
                        config.getBigIntUnsignedHandlingMode().asBigIntUnsignedMode(), config.binaryHandlingMode(),
                        MySqlValueConverters::adjustTemporal, config.getEventConvertingFailureHandlingMode(), config.getServiceRegistry(),
                        config.getUnavailableValuePlaceholder()),
                config.getTopicNamingStrategy(BinlogConnectorConfig.TOPIC_NAMING_STRATEGY), config.schemaNameAdjuster(), false,
                new CustomConverterRegistry(List.of()), taskContext);
    }

    @Override
    protected MySqlPartition createPartition() {
        return new MySqlPartition(database.getServerName(), database.getDatabaseName());
    }

    @Override
    protected MySqlOffsetContext createOffset() {
        return MySqlOffsetContext.initial((MySqlConnectorConfig) config);
    }

    @Override
    protected AbstractIncrementalSnapshotChangeEventSource<MySqlPartition, TableId> createReadOnlySource() {
        return new MySqlReadOnlyIncrementalSnapshotChangeEventSource((MySqlConnectorConfig) config, jdbc, dispatcher,
                (MySqlDatabaseSchema) schema, Clock.system(), SnapshotProgressListener.NO_OP(), DataChangeEventListener.NO_OP(), notifications);
    }
}
