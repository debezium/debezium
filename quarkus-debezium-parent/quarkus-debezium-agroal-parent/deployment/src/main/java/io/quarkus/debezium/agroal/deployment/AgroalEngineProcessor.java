/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.agroal.deployment;

import java.util.List;

import jakarta.inject.Singleton;

import io.quarkus.agroal.spi.JdbcDataSourceBuildItem;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.debezium.agroal.configuration.AgroalDatasourceConfiguration;
import io.quarkus.debezium.agroal.configuration.AgroalDatasourceRecorder;
import io.quarkus.debezium.agroal.engine.AgroalParser;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;

public class AgroalEngineProcessor {

    @BuildStep
    public void parser(BuildProducer<AdditionalBeanBuildItem> additionalBeanProducer) {
        additionalBeanProducer.produce(AdditionalBeanBuildItem
                .builder()
                .addBeanClasses(AgroalParser.class)
                .build());
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    public void produceAgroalDatasourceConfigurations(List<JdbcDataSourceBuildItem> jdbcDataSources,
                                                      AgroalDatasourceRecorder recorder,
                                                      BuildProducer<SyntheticBeanBuildItem> producer) {
        jdbcDataSources.forEach(item -> producer.produce(SyntheticBeanBuildItem
                .configure(AgroalDatasourceConfiguration.class)
                .scope(Singleton.class)
                .supplier(recorder.convert(item.getName(), item.isDefault(), item.getDbKind()))
                .setRuntimeInit()
                .named(item.getDbKind() + item.getName())
                .done()));
    }
}
