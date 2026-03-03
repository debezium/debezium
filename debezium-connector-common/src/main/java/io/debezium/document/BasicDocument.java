/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.util.Iterators;
import io.debezium.util.MathOps;

/**
 * Package-level implementation of {@link Document}.
 *
 * @author Randall Hauch
 */
@NotThreadSafe
final class BasicDocument implements Document {

    static final Function<Map.Entry<? extends CharSequence, Value>, Field> CONVERT_ENTRY_TO_FIELD = new Function<Map.Entry<? extends CharSequence, Value>, Field>() {
        @Override
        public Field apply(Entry<? extends CharSequence, Value> entry) {
            return new BasicField(entry.getKey(), entry.getValue());
        }
    };

    private final Map<CharSequence, Value> fields = new LinkedHashMap<>();

    BasicDocument() {
    }

    @Override
    public int size() {
        return fields.size();
    }

    @Override
    public boolean isEmpty() {
        return fields.isEmpty();
    }

    @Override
    public int compareTo(Document that) {
        return compareTo(that, true);
    }

    @Override
    public int compareToUsingSimilarFields(Document that) {
        if (that == null) {
            return 1;
        }
        int diff = 0;
        // We don't care about order, so just go through by this Document's fields ...
        for (Map.Entry<CharSequence, Value> entry : fields.entrySet()) {
            CharSequence key = entry.getKey();
            diff = compareNonNull(this.get(key), that.get(key));
            if (diff != 0) {
                return diff;
            }
        }
        return 0;
    }

    @Override
    public int compareToWithoutFieldOrder(Document that) {
        return compareTo(that, false);
    }

    @Override
    public int compareTo(Document that, boolean enforceFieldOrder) {
        if (that == null) {
            return 1;
        }
        if (this.size() != that.size()) {
            return this.size() - that.size();
        }
        int diff = 0;
        if (enforceFieldOrder) {
            Iterator<CharSequence> thisIter = this.keySet().iterator(); // ordered
            Iterator<CharSequence> thatIter = that.keySet().iterator(); // ordered
            while (thisIter.hasNext() && thatIter.hasNext()) {
                String thisKey = thisIter.next().toString();
                String thatKey = thatIter.next().toString();
                diff = thisKey.compareTo(thatKey);
                if (diff != 0) {
                    return diff;
                }
                diff = compare(this.get(thisKey), that.get(thatKey));
                if (diff != 0) {
                    return diff;
                }
            }
            if (thisIter.hasNext()) {
                return 1;
            }
            if (thatIter.hasNext()) {
                return -1;
            }
        }
        else {
            // We don't care about order, so just go through by this Document's fields ...
            for (Map.Entry<CharSequence, Value> entry : fields.entrySet()) {
                CharSequence key = entry.getKey();
                diff = compare(this.get(key), that.get(key));
                if (diff != 0) {
                    return diff;
                }
            }
            if (that.size() > this.size()) {
                return 1;
            }
        }
        return 0;
    }

    /**
     * Semantically compare two values. This includes comparing numeric values of different types (e.g., an integer and long),
     * and {@code null} and {@link Value#nullValue()} references.
     *
     * @param value1 the first value; may be null
     * @param value2 the second value; may be null
     * @return a negative integer, zero, or a positive integer as this object
     *         is less than, equal to, or greater than the specified object.
     */
    protected int compare(Value value1, Value value2) {
        if (value1 == null) {
            return Value.isNull(value2) ? 0 : 1;
        }
        return value1.comparable().compareTo(value2.comparable());
    }

    /**
     * Semantically compare two non-null values. This includes comparing numeric values of different types
     * (e.g., an integer and long), but excludes {@code null} and {@link Value#nullValue()} references.
     *
     * @param value1 the first value; may be null
     * @param value2 the second value; may be null
     * @return a negative integer, zero, or a positive integer as this object
     *         is less than, equal to, or greater than the specified object.
     */
    protected int compareNonNull(Value value1, Value value2) {
        if (Value.isNull(value1) || Value.isNull(value2)) {
            return 0;
        }
        return value1.comparable().compareTo(value2.comparable());
    }

    @Override
    public Iterable<CharSequence> keySet() {
        return fields.keySet();
    }

    @Override
    public Iterator<Field> iterator() {
        return Iterators.around(fields.entrySet(), CONVERT_ENTRY_TO_FIELD);
    }

    @Override
    public void clear() {
        fields.clear();
    }

    @Override
    public boolean has(CharSequence fieldName) {
        return fields.containsKey(fieldName);
    }

    @Override
    public boolean hasAll(Document that) {
        if (that == null) {
            return true;
        }
        if (this.size() < that.size()) {
            // Can't have all of 'that' if 'that' is bigger ...
            return false;
        }
        return that.stream().allMatch(field -> {
            Value thatValue = field.getValue();
            Value thisValue = this.get(field.getName());
            return Value.compareTo(thisValue, thatValue) == 0;
        });
    }

    @Override
    public Value get(CharSequence fieldName, Comparable<?> defaultValue) {
        Value value = fields.get(fieldName);
        return value != null ? value : Value.create(defaultValue);
    }

    @Override
    public Document putAll(Iterable<Field> object) {
        object.forEach(this::setValue);
        return this;
    }

    @Override
    public Document removeAll() {
        fields.clear();
        return this;
    }

    @Override
    public Value remove(CharSequence name) {
        if (!fields.containsKey(name)) {
            return null;
        }
        Comparable<?> removedValue = fields.remove(name);
        return Value.create(removedValue);
    }

    @Override
    public Document setValue(CharSequence name, Value value) {
        this.fields.put(name, value != null ? value.clone() : Value.nullValue());
        return this;
    }

    @Override
    public Document increment(CharSequence name, Value increment) {
        if (!increment.isNumber()) {
            throw new IllegalArgumentException("The increment must be a number but is " + increment);
        }
        if (fields.containsKey(name)) {
            Number current = getNumber(name);
            if (current != null) {
                Value updated = Value.create(MathOps.add(current, increment.asNumber()));
                setValue(name, Value.create(updated));
            }
        }
        else {
            setValue(name, increment);
        }
        return this;
    }

    @Override
    public Document clone() {
        return new BasicDocument().putAll(this);
    }

    @Override
    public int hashCode() {
        return fields.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BasicDocument) {
            BasicDocument that = (BasicDocument) obj;
            return fields.equals(that.fields);
        }
        if (obj instanceof Document) {
            Document that = (Document) obj;
            return this.hasAll(that) && that.hasAll(this);
        }
        return false;
    }

    @Override
    public String toString() {
        try {
            return DocumentWriter.prettyWriter().write(this);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
