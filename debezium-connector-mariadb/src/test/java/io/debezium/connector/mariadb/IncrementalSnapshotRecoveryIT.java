/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import java.util.List;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogIncrementalSnapshotRecoveryIT;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.mariadb.jdbc.MariaDbConnection;
import io.debezium.connector.mariadb.jdbc.MariaDbConnectionConfiguration;
import io.debezium.connector.mariadb.jdbc.MariaDbFieldReader;
import io.debezium.connector.mariadb.jdbc.MariaDbValueConverters;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

class IncrementalSnapshotRecoveryIT
        extends BinlogIncrementalSnapshotRecoveryIT<MariaDbConnector, MariaDbPartition, MariaDbOffsetContext>
        implements MariaDbCommon {

    @Override
    protected MariaDbConnectorConfig createConfig(Configuration configuration) {
        return new MariaDbConnectorConfig(configuration);
    }

    @Override
    protected BinlogConnectorConnection createConnection(Configuration configuration) {
        return new MariaDbConnection(new MariaDbConnectionConfiguration(configuration),
                new MariaDbFieldReader((MariaDbConnectorConfig) config));
    }

    @Override
    protected MariaDbTaskContext createTaskContext(Configuration configuration) {
        return new MariaDbTaskContext(configuration, (MariaDbConnectorConfig) config);
    }

    @Override
    protected MariaDbDatabaseSchema createSchema(CdcSourceTaskContext<?> taskContext) {
        return new MariaDbDatabaseSchema((MariaDbConnectorConfig) config,
                new MariaDbValueConverters(config.getDecimalMode(), config.getTemporalPrecisionMode(),
                        config.getBigIntUnsignedHandlingMode().asBigIntUnsignedMode(), config.binaryHandlingMode(),
                        MariaDbValueConverters::adjustTemporal, config.getEventConvertingFailureHandlingMode(), config.getServiceRegistry()),
                config.getTopicNamingStrategy(BinlogConnectorConfig.TOPIC_NAMING_STRATEGY), config.schemaNameAdjuster(), false,
                new CustomConverterRegistry(List.of()), taskContext);
    }

    @Override
    protected MariaDbPartition createPartition() {
        return new MariaDbPartition(database.getServerName(), database.getDatabaseName());
    }

    @Override
    protected MariaDbOffsetContext createOffset() {
        return MariaDbOffsetContext.initial((MariaDbConnectorConfig) config);
    }

    @Override
    protected AbstractIncrementalSnapshotChangeEventSource<MariaDbPartition, TableId> createReadOnlySource() {
        return new MariaDbReadOnlyIncrementalSnapshotChangeEventSource((MariaDbConnectorConfig) config, jdbc, dispatcher,
                (MariaDbDatabaseSchema) schema, Clock.system(), SnapshotProgressListener.NO_OP(), DataChangeEventListener.NO_OP(), notifications);
    }
}
