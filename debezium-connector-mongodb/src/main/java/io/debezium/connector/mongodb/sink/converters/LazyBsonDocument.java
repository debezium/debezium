/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.converters;

import static java.lang.String.format;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.lang.Nullable;

/**
 * A lazy representation of a MongoDB document.
 */
public class LazyBsonDocument extends BsonDocument {
    private static final long serialVersionUID = 1L;

    private final transient SinkRecord record;
    private final transient Type dataType;
    private final transient BiFunction<Schema, Object, BsonDocument> converter;

    private BsonDocument unwrapped;

    public enum Type {
        KEY,
        VALUE
    }

    /**
     * Construct a new instance with the given supplier of the document.
     *
     * @param record the sink record to convert
     * @param converter the converter for the sink record
     */
    public LazyBsonDocument(
                            final SinkRecord record,
                            final Type dataType,
                            final BiFunction<Schema, Object, BsonDocument> converter) {
        if (record == null) {
            throw new IllegalArgumentException("record can not be null");
        }
        else if (dataType == null) {
            throw new IllegalArgumentException("dataType can not be null");
        }
        else if (converter == null) {
            throw new IllegalArgumentException("converter can not be null");
        }
        this.record = record;
        this.dataType = dataType;
        this.converter = converter;
    }

    @Override
    public int size() {
        return getUnwrapped().size();
    }

    @Override
    public boolean isEmpty() {
        return getUnwrapped().isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
        return getUnwrapped().containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return getUnwrapped().containsValue(value);
    }

    @Override
    public BsonValue get(final Object key) {
        return getUnwrapped().get(key);
    }

    @Override
    public BsonValue put(final String key, final BsonValue value) {
        return getUnwrapped().put(key, value);
    }

    @Override
    public BsonValue remove(final Object key) {
        return getUnwrapped().remove(key);
    }

    @Override
    public void putAll(final Map<? extends String, ? extends BsonValue> m) {
        getUnwrapped().putAll(m);
    }

    @Override
    public void clear() {
        getUnwrapped().clear();
    }

    @Override
    public Set<String> keySet() {
        return getUnwrapped().keySet();
    }

    @Override
    public Collection<BsonValue> values() {
        return getUnwrapped().values();
    }

    @Override
    public Set<Entry<String, BsonValue>> entrySet() {
        return getUnwrapped().entrySet();
    }

    @Override
    public boolean equals(final Object o) {
        return getUnwrapped().equals(o);
    }

    @Override
    public int hashCode() {
        return getUnwrapped().hashCode();
    }

    @Override
    public String toString() {
        return getUnwrapped().toString();
    }

    @Override
    public BsonDocument clone() {
        return unwrapped != null
                ? unwrapped.clone()
                : new LazyBsonDocument(record, dataType, converter);
    }

    private BsonDocument getUnwrapped() {
        if (unwrapped == null) {
            switch (dataType) {
                case KEY:
                    try {
                        unwrapped = converter.apply(record.keySchema(), record.key());
                    }
                    catch (Exception e) {
                        throw new DataException(
                                format(
                                        "Could not convert key %s into a BsonDocument.",
                                        unambiguousToString(record.key())),
                                e);
                    }
                    break;
                case VALUE:
                    try {
                        unwrapped = converter.apply(record.valueSchema(), record.value());
                    }
                    catch (Exception e) {
                        throw new DataException(
                                format(
                                        "Could not convert value %s into a BsonDocument.",
                                        unambiguousToString(record.value())),
                                e);
                    }
                    break;
                default:
                    throw new DataException(format("Unknown data type %s.", dataType));
            }
        }
        return unwrapped;
    }

    // see https://docs.oracle.com/javase/6/docs/platform/serialization/spec/output.html
    private Object writeReplace() {
        return getUnwrapped();
    }

    // see https://docs.oracle.com/javase/6/docs/platform/serialization/spec/input.html
    private void readObject(final ObjectInputStream stream) throws InvalidObjectException {
        throw new InvalidObjectException("Proxy required");
    }

    private static String unambiguousToString(@Nullable final Object v) {
        String stringValue = String.valueOf(v);
        if (v == null) {
            return format("'%s' (null reference)", stringValue);
        }
        else if (stringValue.equals(String.valueOf((Object) null))) {
            return format("'%s' (%s, not a null reference)", stringValue, v.getClass().getName());
        }
        else {
            return format("'%s' (%s)", stringValue, v.getClass().getName());
        }
    }
}
