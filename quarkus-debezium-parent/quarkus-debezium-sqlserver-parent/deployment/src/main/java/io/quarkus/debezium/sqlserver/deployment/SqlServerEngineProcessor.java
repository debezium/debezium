/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.sqlserver.deployment;

import io.debezium.connector.sqlserver.Module;
import io.debezium.connector.sqlserver.SqlServerConnector;
import io.debezium.connector.sqlserver.SqlServerConnectorTask;
import io.debezium.connector.sqlserver.SqlServerSourceInfoStructMaker;
import io.debezium.connector.sqlserver.snapshot.lock.ExclusiveSnapshotLock;
import io.debezium.connector.sqlserver.snapshot.lock.NoSnapshotLock;
import io.debezium.connector.sqlserver.snapshot.query.SelectAllSnapshotQuery;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;
import io.quarkus.debezium.agroal.configuration.AgroalDatasourceConfiguration;
import io.quarkus.debezium.deployment.QuarkusEngineProcessor;
import io.quarkus.debezium.deployment.items.DebeziumConnectorBuildItem;
import io.quarkus.debezium.deployment.items.DebeziumExtensionNameBuildItem;
import io.quarkus.debezium.engine.SqlServerEngineProducer;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.pkg.steps.NativeOrNativeSourcesBuild;

public class SqlServerEngineProcessor implements QuarkusEngineProcessor<AgroalDatasourceConfiguration> {

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
    public Class<AgroalDatasourceConfiguration> quarkusDatasourceConfiguration() {
        return AgroalDatasourceConfiguration.class;
    }

}
