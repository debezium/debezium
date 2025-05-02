/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.postgres.deployment;

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.processor.DotNames;
import io.quarkus.debezium.producer.PostgresEngineProducer;
import io.quarkus.debezium.recorder.DebeziumRecorder;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.ApplicationStartBuildItem;
import io.quarkus.deployment.builditem.ExecutorBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.ShutdownContextBuildItem;

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

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void startEngine(ApplicationStartBuildItem ignore,
                     DebeziumRecorder recorder,
                     ExecutorBuildItem executorBuildItem,
                     ShutdownContextBuildItem shutdownContextBuildItem) {

        recorder.startEngine(executorBuildItem.getExecutorProxy(), shutdownContextBuildItem);
    }

}
