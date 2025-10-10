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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.mongodb.runtime.MongodbConfig;
import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class MongoDbDatasourceRecorder implements DatasourceRecorder<MultiEngineMongoDbDatasourceConfiguration> {

    private final RuntimeValue<MongodbConfig> runtimeConfig;
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbDatasourceRecorder.class);

    public MongoDbDatasourceRecorder(final RuntimeValue<MongodbConfig> runtimeConfig) {
        this.runtimeConfig = runtimeConfig;
    }

    @Override
    public Supplier<MultiEngineMongoDbDatasourceConfiguration> convert(String name, boolean defaultConfiguration) {

        Map<String, MongoDbDatasourceConfiguration> configurations = runtimeConfig.getValue()
                .mongoClientConfigs()
                .entrySet().stream()
                .filter(a -> !a.getKey().equals("health"))
                .map(config -> new MongoDbDatasourceConfiguration(
                        config.getValue().connectionString().get(),
                        config.getKey(),
                        false))
                .collect(Collectors.toMap(MongoDbDatasourceConfiguration::getSanitizedName, Function.identity()));

        configurations.put("default", new MongoDbDatasourceConfiguration(runtimeConfig.getValue().defaultMongoClientConfig().connectionString().get(), "default", true));

        return () -> new MultiEngineMongoDbDatasourceConfiguration(configurations);
    }
}
