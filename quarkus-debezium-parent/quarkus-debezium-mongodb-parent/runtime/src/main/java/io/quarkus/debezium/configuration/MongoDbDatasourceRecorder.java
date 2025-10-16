/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.configuration;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.debezium.runtime.configuration.QuarkusDatasourceConfiguration;
import io.debezium.runtime.recorder.DatasourceRecorder;
import io.quarkus.mongodb.runtime.MongodbConfig;
import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;

import static io.debezium.runtime.configuration.QuarkusDatasourceConfiguration.DEFAULT;

@Recorder
public class MongoDbDatasourceRecorder implements DatasourceRecorder<MultiEngineMongoDbDatasourceConfiguration> {

    /**
     * The mongodb-client extension contains health configurations which are not necessary for the instrumentation
     * of Debezium Engine
     */
    public static final String TO_REMOVE = "health";
    private final RuntimeValue<MongodbConfig> runtimeConfig;

    public MongoDbDatasourceRecorder(final RuntimeValue<MongodbConfig> runtimeConfig) {
        this.runtimeConfig = runtimeConfig;
    }

    @Override
    public Supplier<MultiEngineMongoDbDatasourceConfiguration> convert(String name, boolean defaultConfiguration) {
        Map<String, MongoDbDatasourceConfiguration> configurations = runtimeConfig.getValue()
                .mongoClientConfigs()
                .entrySet().stream()
                .filter(entry -> !entry.getKey().equals(TO_REMOVE))
                .map(config -> new MongoDbDatasourceConfiguration(
                        config.getValue().connectionString().get(),
                        config.getKey(),
                        false))
                .collect(Collectors.toMap(MongoDbDatasourceConfiguration::getSanitizedName, Function.identity()));

        configurations.put(DEFAULT,
                new MongoDbDatasourceConfiguration(runtimeConfig.getValue().defaultMongoClientConfig().connectionString().get(),
                        DEFAULT,
                        true));

        return () -> new MultiEngineMongoDbDatasourceConfiguration(configurations);
    }
}
