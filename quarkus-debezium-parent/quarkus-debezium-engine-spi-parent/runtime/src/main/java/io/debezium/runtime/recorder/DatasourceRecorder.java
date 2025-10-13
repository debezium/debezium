/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.runtime.recorder;

import java.util.function.Supplier;

import io.debezium.runtime.configuration.QuarkusDatasourceConfiguration;
import io.quarkus.runtime.annotations.Recorder;

/**
 * Given a reference to a configuration returns a {@link QuarkusDatasourceConfiguration}
 * @param <T>
 */
@Recorder
public interface DatasourceRecorder<T extends QuarkusDatasourceConfiguration> {
    Supplier<T> convert(String name, boolean isDefault);
}
