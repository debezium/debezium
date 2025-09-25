/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.configuration;

import java.util.function.Supplier;

import io.quarkus.mongodb.runtime.MongodbConfig;
import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class MongoDbDatasourceRecorder implements DatasourceRecorder<MongoDbDatasourceConfiguration> {

    private final RuntimeValue<MongodbConfig> runtimeConfig;

    public MongoDbDatasourceRecorder(final RuntimeValue<MongodbConfig> runtimeConfig) {
        this.runtimeConfig = runtimeConfig;
    }

    @Override
    public Supplier<MongoDbDatasourceConfiguration> convert(String name, boolean defaultConfiguration) {
        return () -> new MongoDbDatasourceConfiguration(runtimeConfig.getValue().defaultMongoClientConfig().connectionString().get(), "default", true);
    }
}
