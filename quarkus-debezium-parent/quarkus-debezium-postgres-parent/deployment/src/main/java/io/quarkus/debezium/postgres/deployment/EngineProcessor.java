/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.postgres.deployment;

import java.net.URI;
import java.util.Map;

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.processor.DotNames;
import io.quarkus.datasource.deployment.spi.DevServicesDatasourceResultBuildItem;
import io.quarkus.debezium.producer.PostgresEngineProducer;
import io.quarkus.debezium.recorder.DebeziumRecorder;
import io.quarkus.deployment.IsNormal;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.ApplicationStartBuildItem;
import io.quarkus.deployment.builditem.DevServicesResultBuildItem;
import io.quarkus.deployment.builditem.ExecutorBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.ShutdownContextBuildItem;
import io.quarkus.deployment.dev.devservices.DevServicesConfig;

public class EngineProcessor {

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem("debezium-postgres");
    }

    @BuildStep
    void engine(BuildProducer<AdditionalBeanBuildItem> additionalBeanProducer) {
        additionalBeanProducer.produce(AdditionalBeanBuildItem
                .builder()
                .addBeanClasses(PostgresEngineProducer.class)
                .setUnremovable()
                .setDefaultScope(DotNames.APPLICATION_SCOPED)
                .build());
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

        URI uri = URI.create(datasource.getConfigProperties().get("quarkus.datasource.jdbc.url").substring(5));
        devServicesProducer.produce(new DevServicesResultBuildItem(
                "debezium-postgres",
                "debezium",
                Map.of(
                        "quarkus.debezium.configuration.database.hostname", uri.getHost(),
                        "quarkus.debezium.configuration.database.user", datasource.getConfigProperties().get("quarkus.datasource.username"),
                        "quarkus.debezium.configuration.database.password", datasource.getConfigProperties().get("quarkus.datasource.password"),
                        "quarkus.debezium.configuration.database.dbname", uri.getPath().substring(1),
                        "quarkus.debezium.configuration.database.port",
                        String.valueOf(uri.getPort()))));
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void startEngine(ApplicationStartBuildItem ignore,
                     DebeziumRecorder recorder,
                     ExecutorBuildItem executorBuildItem,
                     ShutdownContextBuildItem shutdownContextBuildItem) {

        recorder.startEngine(executorBuildItem.getExecutorProxy(), shutdownContextBuildItem);
    }

}
