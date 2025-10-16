/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

import io.debezium.common.annotation.Incubating;
import io.debezium.runtime.configuration.QuarkusDatasourceConfiguration;

@Incubating
public record EngineManifest(String id) {
    public static EngineManifest DEFAULT = new EngineManifest(QuarkusDatasourceConfiguration.DEFAULT);
}
