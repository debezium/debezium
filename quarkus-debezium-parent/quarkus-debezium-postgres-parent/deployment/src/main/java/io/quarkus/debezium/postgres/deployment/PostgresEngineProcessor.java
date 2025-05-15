/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.postgres.deployment;

import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.postgresql.PostgresConnectorTask;
import io.debezium.connector.postgresql.PostgresSourceInfoStructMaker;
import io.debezium.connector.postgresql.snapshot.lock.NoSnapshotLock;
import io.debezium.connector.postgresql.snapshot.lock.SharedSnapshotLock;
import io.debezium.connector.postgresql.snapshot.query.SelectAllSnapshotQuery;
import io.quarkus.datasource.deployment.spi.DevServicesDatasourceResultBuildItem;
import io.quarkus.debezium.deployment.items.DebeziumConnectorBuildItem;
import io.quarkus.debezium.engine.PostgresEngineProducer;
import io.quarkus.deployment.IsNormal;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.DevServicesResultBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.dev.devservices.DevServicesConfig;
import io.quarkus.deployment.pkg.steps.NativeOrNativeSourcesBuild;

public class PostgresEngineProcessor {

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem("debezium-postgres");
    }

    @BuildStep
    public DebeziumConnectorBuildItem engine() {
        return new DebeziumConnectorBuildItem("postgresql", PostgresEngineProducer.class);
    }

    @BuildStep(onlyIfNot = IsNormal.class, onlyIf = DevServicesConfig.Enabled.class)
    void configure(BuildProducer<DevServicesResultBuildItem> devServicesProducer,
                   DevServicesDatasourceResultBuildItem devServicesDatasourceResultBuildItem) {
        DevServicesDatasourceResultBuildItem.DbResult datasource = devServicesDatasourceResultBuildItem.getDefaultDatasource();

        if (datasource == null) {
            return;
        }

        if (!datasource.getDbType().equals("postgresql")) {
            return;
        }

        devServicesProducer.produce(new DevServicesResultBuildItem("debezium-postgres",
                "debezium",
                QuarkusDatasource.generateDebeziumConfiguration(datasource.getConfigProperties())));
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    void registerClassesThatAreLoadedThroughReflection(BuildProducer<ReflectiveClassBuildItem> reflectiveClasses) {
        reflectiveClasses.produce(ReflectiveClassBuildItem.builder(
                PostgresConnector.class,
                PostgresSourceInfoStructMaker.class,
                PostgresConnectorTask.class,
                NoSnapshotLock.class,
                SharedSnapshotLock.class,
                SelectAllSnapshotQuery.class)
                .reason(getClass().getName())
                .build());
    }

}
