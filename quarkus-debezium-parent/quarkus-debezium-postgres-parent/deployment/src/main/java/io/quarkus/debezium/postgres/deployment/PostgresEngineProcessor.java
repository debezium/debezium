/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.postgres.deployment;

import java.util.List;

import jakarta.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.Module;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.postgresql.PostgresConnectorTask;
import io.debezium.connector.postgresql.PostgresSourceInfoStructMaker;
import io.debezium.connector.postgresql.snapshot.lock.NoSnapshotLock;
import io.debezium.connector.postgresql.snapshot.lock.SharedSnapshotLock;
import io.debezium.connector.postgresql.snapshot.query.SelectAllSnapshotQuery;
import io.debezium.runtime.configuration.QuarkusDatasourceConfiguration;
import io.quarkus.agroal.spi.JdbcDataSourceBuildItem;
import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.datasource.deployment.spi.DevServicesDatasourceResultBuildItem;
import io.quarkus.debezium.configuration.DatasourceRecorder;
import io.quarkus.debezium.deployment.items.DebeziumConnectorBuildItem;
import io.quarkus.debezium.engine.PostgresEngineProducer;
import io.quarkus.deployment.IsNormal;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.DevServicesResultBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.dev.devservices.DevServicesConfig;
import io.quarkus.deployment.pkg.steps.NativeOrNativeSourcesBuild;

public class PostgresEngineProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresEngineProcessor.class.getName());
    public static final String POSTGRESQL = Module.name();

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem("debezium-" + POSTGRESQL);
    }

    @BuildStep
    public DebeziumConnectorBuildItem engine() {
        return new DebeziumConnectorBuildItem(POSTGRESQL, PostgresEngineProducer.class);
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    public void generateDatasourceConfig(
                                         DatasourceRecorder datasourceRecorder,
                                         BuildProducer<SyntheticBeanBuildItem> producer,
                                         List<JdbcDataSourceBuildItem> jdbcDataSources) {

        jdbcDataSources
                .stream()
                .filter(item -> item.getDbKind().equals(POSTGRESQL))
                .forEach(item -> producer.produce(SyntheticBeanBuildItem
                        .configure(QuarkusDatasourceConfiguration.class)
                        .scope(Singleton.class)
                        .supplier(datasourceRecorder.convert(item.getName(), item.isDefault()))
                        .setRuntimeInit()
                        .named(item.getDbKind() + item.getName())
                        .done()));
    }

    @BuildStep(onlyIfNot = IsNormal.class, onlyIf = DevServicesConfig.Enabled.class)
    void configure(BuildProducer<DevServicesResultBuildItem> devServicesProducer,
                   DevServicesDatasourceResultBuildItem devServicesDatasourceResultBuildItem) {
        DevServicesDatasourceResultBuildItem.DbResult datasource = devServicesDatasourceResultBuildItem.getDefaultDatasource();

        if (datasource == null) {
            return;
        }

        if (!datasource.getDbType().equals(POSTGRESQL)) {
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
