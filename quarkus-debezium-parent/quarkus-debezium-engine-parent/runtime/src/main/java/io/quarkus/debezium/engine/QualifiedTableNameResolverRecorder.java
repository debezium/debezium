/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.util.function.Supplier;

import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class QualifiedTableNameResolverRecorder {

    public Supplier<FullyQualifiedTableNameResolver> get() {
        return DefaultQualifiedTableNameResolver::new;
    }
}
