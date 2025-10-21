/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.sqlserver.deployment;

import static io.quarkus.datasource.common.runtime.DatabaseKind.SupportedDatabaseKind.MSSQL;

import java.util.List;

import io.debezium.connector.sqlserver.Module;
import io.debezium.connector.sqlserver.SqlServerConnector;
import io.debezium.connector.sqlserver.SqlServerConnectorTask;
import io.debezium.connector.sqlserver.SqlServerSourceInfoStructMaker;
import io.debezium.connector.sqlserver.snapshot.lock.ExclusiveSnapshotLock;
import io.debezium.connector.sqlserver.snapshot.lock.NoSnapshotLock;
import io.debezium.connector.sqlserver.snapshot.query.SelectAllSnapshotQuery;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;
import io.quarkus.agroal.spi.JdbcDataSourceBuildItem;
import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.debezium.deployment.QuarkusEngineProcessor;
import io.quarkus.debezium.deployment.items.DebeziumConnectorBuildItem;
import io.quarkus.debezium.deployment.items.DebeziumExtensionNameBuildItem;
import io.quarkus.debezium.engine.SqlServerEngineProducer;
import io.quarkus.debezium.sqlserver.configuration.SqlServerDatasourceConfiguration;
import io.quarkus.debezium.sqlserver.configuration.SqlServerDatasourceRecorder;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.pkg.steps.NativeOrNativeSourcesBuild;

public class SqlServerEngineProcessor implements QuarkusEngineProcessor<SqlServerDatasourceConfiguration> {

    private static final String SQLSERVER = Module.name();

    @BuildStep
    @Override
    public DebeziumExtensionNameBuildItem debeziumExtensionNameBuildItem() {
        return new DebeziumExtensionNameBuildItem(SQLSERVER);
    }

    @BuildStep
    @Override
    public DebeziumConnectorBuildItem engine() {
        return new DebeziumConnectorBuildItem(SQLSERVER, SqlServerEngineProducer.class);
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    @Override
    public void registerClassesThatAreLoadedThroughReflection(BuildProducer<ReflectiveClassBuildItem> reflectiveClassBuildItemBuildProducer) {
        reflectiveClassBuildItemBuildProducer.produce(ReflectiveClassBuildItem.builder(
                SchemaHistory.class,
                KafkaSchemaHistory.class,
                SqlServerConnector.class,
                SqlServerSourceInfoStructMaker.class,
                SqlServerConnectorTask.class,
                NoSnapshotLock.class,
                ExclusiveSnapshotLock.class,
                SelectAllSnapshotQuery.class)
                .reason(getClass().getName())
                .build());
    }

    @Override
    public Class<SqlServerDatasourceConfiguration> quarkusDatasourceConfiguration() {
        return SqlServerDatasourceConfiguration.class;
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    public void produceSqlServerDatasourceConfig(List<JdbcDataSourceBuildItem> jdbcDataSources,
                                                 SqlServerDatasourceRecorder recorder,
                                                 BuildProducer<SyntheticBeanBuildItem> producer) {
        jdbcDataSources
                .stream()
                .filter(item -> item.getDbKind().equals(MSSQL.getMainName()))
                .forEach(item -> produceQuarkusDatasourceConfiguration(
                        recorder.convert(item.getName(), item.isDefault()),
                        producer,
                        item.getDbKind() + item.getName()));
    }
}
