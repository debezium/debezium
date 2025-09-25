/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.configuration;

import java.util.function.Supplier;

import io.debezium.runtime.configuration.QuarkusDatasourceConfiguration;

public interface DatasourceRecorder<T extends QuarkusDatasourceConfiguration> {
    Supplier<T> convert(String name, boolean isDefault);
}
