/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.mariadb.deployment;

import io.debezium.connector.binlog.charset.BinlogCharsetRegistry;
import io.debezium.connector.binlog.snapshot.lock.MinimalAtLeastOnceSnapshotLock;
import io.debezium.connector.mariadb.MariaDbConnector;
import io.debezium.connector.mariadb.MariaDbConnectorTask;
import io.debezium.connector.mariadb.MariaDbSourceInfoStructMaker;
import io.debezium.connector.mariadb.Module;
import io.debezium.connector.mariadb.charset.MariaDbCharsetRegistry;
import io.debezium.connector.mariadb.snapshot.lock.DefaultSnapshotLock;
import io.debezium.connector.mariadb.snapshot.lock.ExtendedSnapshotLock;
import io.debezium.connector.mariadb.snapshot.lock.MinimalSnapshotLock;
import io.debezium.connector.mariadb.snapshot.lock.NoneSnapshotLock;
import io.debezium.connector.mariadb.snapshot.query.SelectAllSnapshotQuery;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;
import io.quarkus.debezium.agroal.configuration.AgroalDatasourceConfiguration;
import io.quarkus.debezium.deployment.QuarkusEngineProcessor;
import io.quarkus.debezium.deployment.items.DebeziumConnectorBuildItem;
import io.quarkus.debezium.deployment.items.DebeziumExtensionNameBuildItem;
import io.quarkus.debezium.engine.MariaDbEngineProducer;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.NativeImageEnableAllCharsetsBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageResourceBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.pkg.steps.NativeOrNativeSourcesBuild;

public class MariaDbEngineProcessor implements QuarkusEngineProcessor<AgroalDatasourceConfiguration> {

    private static final String MARIADB = Module.name();

    @BuildStep
    @Override
    public DebeziumExtensionNameBuildItem debeziumExtensionNameBuildItem() {
        return new DebeziumExtensionNameBuildItem(MARIADB);
    }

    @BuildStep
    @Override
    public DebeziumConnectorBuildItem engine() {
        return new DebeziumConnectorBuildItem(MARIADB, MariaDbEngineProducer.class);
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    @Override
    public void registerClassesThatAreLoadedThroughReflection(BuildProducer<ReflectiveClassBuildItem> reflectiveClassBuildItemBuildProducer) {

        reflectiveClassBuildItemBuildProducer.produce(ReflectiveClassBuildItem.builder(
                "io.debezium.connector.mariadb.charset.MariaDbCharsetRegistry$CharacterSetMappings",
                "io.debezium.connector.mariadb.charset.MariaDbCharsetRegistry$CharacterSetMapping",
                "io.debezium.connector.mariadb.charset.MariaDbCharsetRegistry$CharacterSetMapping",
                "io.debezium.connector.mariadb.charset.MariaDbCharsetRegistry$CollationMapping")
                .build());

        reflectiveClassBuildItemBuildProducer.produce(ReflectiveClassBuildItem.builder(
                SchemaHistory.class,
                KafkaSchemaHistory.class,
                MariaDbConnector.class,
                MariaDbSourceInfoStructMaker.class,
                MariaDbConnectorTask.class,
                MinimalAtLeastOnceSnapshotLock.class,
                DefaultSnapshotLock.class,
                ExtendedSnapshotLock.class,
                MinimalSnapshotLock.class,
                NoneSnapshotLock.class,
                MariaDbCharsetRegistry.class,
                BinlogCharsetRegistry.class,
                SelectAllSnapshotQuery.class)
                .reason(getClass().getName())
                .build());
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    void registerCharsets(BuildProducer<NativeImageResourceBuildItem> resources) {
        resources.produce(new NativeImageResourceBuildItem("charset_mappings.json"));
    }

    @Override
    public Class<AgroalDatasourceConfiguration> quarkusDatasourceConfiguration() {
        return AgroalDatasourceConfiguration.class;
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    public NativeImageEnableAllCharsetsBuildItem enableAllCharsets() {
        return new NativeImageEnableAllCharsetsBuildItem();
    }
}
