/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.Builder;
import io.debezium.engine.DebeziumEngine.BuilderFactory;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.engine.format.KeyValueChangeEventFormat;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import io.debezium.engine.format.SerializationFormat;

public class ConvertingEngineBuilderFactory implements BuilderFactory {

    @Override
    public <T, V extends SerializationFormat<T>> Builder<RecordChangeEvent<T>> builder(ChangeEventFormat<V> format) {
        return new ConvertingEngineBuilder<>(format);
    }

    @Override
    public <S, T, K extends SerializationFormat<S>, V extends SerializationFormat<T>> Builder<ChangeEvent<S, T>> builder(
                                                                                                                         KeyValueChangeEventFormat<K, V> format) {
        return new ConvertingEngineBuilder<>(format);
    }

    public <S, T, U, K extends SerializationFormat<S>, V extends SerializationFormat<T>, H extends SerializationFormat<U>> Builder<ChangeEvent<S, T>> builder(
                                                                                                                                                              KeyValueHeaderChangeEventFormat<K, V, H> format) {
        return new ConvertingEngineBuilder<>(format);
    }
}
