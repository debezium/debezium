/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.engine.format.KeyValueChangeEventFormat;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import io.debezium.engine.format.SerializationFormat;

/**
 * Implementation of {@link DebeziumEngine.BuilderFactory} for {@link AsyncEmbeddedEngine}.
 *
 * @author vjuranek
 */
public class ConvertingAsyncEngineBuilderFactory implements DebeziumEngine.BuilderFactory {

    @Override
    public <T, V extends SerializationFormat<T>> DebeziumEngine.Builder<RecordChangeEvent<T>> builder(ChangeEventFormat<V> format) {
        return new AsyncEmbeddedEngine.AsyncEngineBuilder<>(format);
    }

    @Override
    public <S, T, K extends SerializationFormat<S>, V extends SerializationFormat<T>> DebeziumEngine.Builder<ChangeEvent<S, T>> builder(
                                                                                                                                        KeyValueChangeEventFormat<K, V> format) {
        return new AsyncEmbeddedEngine.AsyncEngineBuilder<>(format);
    }

    public <S, T, U, K extends SerializationFormat<S>, V extends SerializationFormat<T>, H extends SerializationFormat<U>> DebeziumEngine.Builder<ChangeEvent<S, T>> builder(
                                                                                                                                                                             KeyValueHeaderChangeEventFormat<K, V, H> format) {
        return new AsyncEmbeddedEngine.AsyncEngineBuilder<>(format);
    }
}
