/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive.internal;

import java.util.Objects;

import io.debezium.config.Configuration;
import io.debezium.embedded.spi.OffsetCommitPolicy;
import io.debezium.reactive.Converter;
import io.debezium.reactive.ReactiveEngine;
import io.debezium.reactive.ReactiveEngine.Builder;
import io.debezium.util.Clock;

/**
 * Default implementation of reactive engine Builder.
 *
 * @author Jiri Pechanec
 *
 */
public class ReactiveEngineBuilderImpl<T> implements Builder<T> {
    private Configuration config;
    private ClassLoader classLoader;
    private Clock clock;
    private OffsetCommitPolicy offsetCommitPolicy = null;
    private Class<? extends Converter<T>> converter;

    @Override
    public Builder<T> withConfiguration(Configuration config) {
        this.config = config;
        return this;
    }

    @Override
    public Builder<T> withClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
        return this;
    }

    @Override
    public Builder<T> withClock(Clock clock) {
        this.clock = clock;
        return this;
    }

    @Override
    public Builder<T> withOffsetCommitPolicy(OffsetCommitPolicy offsetCommitPolicy) {
        this.offsetCommitPolicy = offsetCommitPolicy;
        return this;
    }

    @Override
    public Builder<T> withConverter(Class<? extends Converter<T>> converter) {
        this.converter = converter;
        return null;
    }

    @Override
    public ReactiveEngine<T> build() {
        if (classLoader == null) classLoader = getClass().getClassLoader();
        if (clock == null) clock = Clock.system();
        Objects.requireNonNull(config, "A connector configuration must be specified.");

        return new ReactiveEngineImpl<T>(this);
    }

    Configuration getConfig() {
        return config;
    }

    ClassLoader getClassLoader() {
        return classLoader;
    }

    Clock getClock() {
        return clock;
    }

    OffsetCommitPolicy getOffsetCommitPolicy() {
        return offsetCommitPolicy;
    }

    Class<? extends Converter<T>> getConverter() {
        return converter;
    }
}