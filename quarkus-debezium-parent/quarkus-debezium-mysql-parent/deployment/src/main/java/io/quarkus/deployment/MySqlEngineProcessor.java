/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.deployment;

import io.debezium.connector.mysql.Module;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorTask;
import io.debezium.connector.mysql.MySqlSourceInfoStructMaker;
import io.debezium.connector.mysql.snapshot.lock.DefaultSnapshotLock;
import io.debezium.connector.mysql.snapshot.lock.ExtendedSnapshotLock;
import io.debezium.connector.mysql.snapshot.lock.MinimalPerconaNoTableLocksSnapshotLock;
import io.debezium.connector.mysql.snapshot.lock.MinimalPerconaSnapshotLock;
import io.debezium.connector.mysql.snapshot.lock.MinimalSnapshotLock;
import io.debezium.connector.mysql.snapshot.lock.NoneSnapshotLock;
import io.debezium.connector.mysql.snapshot.query.SelectAllSnapshotQuery;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;
import io.quarkus.debezium.agroal.configuration.AgroalDatasourceConfiguration;
import io.quarkus.debezium.deployment.QuarkusEngineProcessor;
import io.quarkus.debezium.deployment.items.DebeziumConnectorBuildItem;
import io.quarkus.debezium.deployment.items.DebeziumExtensionNameBuildItem;
import io.quarkus.debezium.engine.MySqlEngineProducer;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.pkg.steps.NativeOrNativeSourcesBuild;

public class MySqlEngineProcessor implements QuarkusEngineProcessor<AgroalDatasourceConfiguration> {

    private static final String MYSQL = Module.name();

    @BuildStep
    @Override
    public DebeziumExtensionNameBuildItem debeziumExtensionNameBuildItem() {
        return new DebeziumExtensionNameBuildItem(MYSQL);
    }

    @BuildStep
    @Override
    public DebeziumConnectorBuildItem engine() {
        return new DebeziumConnectorBuildItem(MYSQL, MySqlEngineProducer.class);
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    @Override
    public void registerClassesThatAreLoadedThroughReflection(BuildProducer<ReflectiveClassBuildItem> reflectiveClassBuildItemBuildProducer) {
        reflectiveClassBuildItemBuildProducer.produce(ReflectiveClassBuildItem.builder(
                SchemaHistory.class,
                KafkaSchemaHistory.class,
                MySqlConnector.class,
                MySqlSourceInfoStructMaker.class,
                MySqlConnectorTask.class,
                DefaultSnapshotLock.class,
                ExtendedSnapshotLock.class,
                MinimalPerconaNoTableLocksSnapshotLock.class,
                MinimalPerconaSnapshotLock.class,
                MinimalSnapshotLock.class,
                NoneSnapshotLock.class,
                SelectAllSnapshotQuery.class)
                .reason(getClass().getName())
                .build());
    }

    @Override
    public Class<AgroalDatasourceConfiguration> quarkusDatasourceConfiguration() {
        return AgroalDatasourceConfiguration.class;
    }
}
