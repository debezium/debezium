/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive.internal;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.spi.OffsetCommitPolicy;
import io.debezium.reactive.ReactiveEngine;
import io.debezium.reactive.ReactiveEngine.Builder;
import io.debezium.util.Clock;

/**
 * Default implementation of reactive engine Builder.
 *
 * @author Jiri Pechanec
 *
 */
public class ReactiveEngineBuilderImpl implements Builder {
    private Configuration config;
    private ClassLoader classLoader;
    private Clock clock;
    private OffsetCommitPolicy offsetCommitPolicy = null;

    @Override
    public Builder using(Configuration config) {
        this.config = config;
        return this;
    }

    @Override
    public Builder using(ClassLoader classLoader) {
        this.classLoader = classLoader;
        return this;
    }

    @Override
    public Builder using(Clock clock) {
        this.clock = clock;
        return this;
    }

    @Override
    public Builder using(OffsetCommitPolicy offsetCommitPolicy) {
        this.offsetCommitPolicy = offsetCommitPolicy;
        return this;
    }

    @Override
    public ReactiveEngine build() {
        final io.debezium.embedded.EmbeddedEngine.Builder engineBuilder = EmbeddedEngine.create().using(config).using(classLoader).using(clock)
                .using(offsetCommitPolicy);
        return new ReactiveEngineImpl(engineBuilder);
    }

}
