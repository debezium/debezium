/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.debezium.util.Iterators;

/**
 * An array of {@link Value}s. The array can also be viewed as a stream of {@link Entry} instances, each of which contain the
 * index and the value.
 *
 * @author Randall Hauch
 *
 */
public interface Array extends Iterable<Array.Entry>, Comparable<Array> {

    static interface Entry extends Comparable<Entry> {

        /**
         * Get the index of the entry
         *
         * @return the entry's index; never null
         */
        int getIndex();

        /**
         * Get the value of the entry.
         *
         * @return the entry's value; may be null
         */
        Value getValue();

        @Override
        default int compareTo(Entry that) {
            if (that == null) {
                return 1;
            }
            int diff = this.getIndex() - that.getIndex();
            if (diff != 0) {
                return diff;
            }
            return this.getValue().compareTo(that.getValue());
        }
    }

    static Array create() {
        return new BasicArray();
    }

    static Array createWithNulls(int number) {
        Value[] vals = new Value[number];
        Arrays.fill(vals, Value.nullValue());
        return new BasicArray(vals);
    }

    static Array create(Object... values) {
        if (values == null || values.length == 0) {
            return create();
        }
        Value[] vals = new Value[values.length];
        for (int i = 0; i != values.length; ++i) {
            vals[i] = Value.create(values[i]);
        }
        return new BasicArray(vals);
    }

    static Array create(Value[] values) {
        if (values == null || values.length == 0) {
            return create();
        }
        return new BasicArray(values);
    }

    static Array create(Value firstValue, Value secondValue, Value... additionalValues) {
        Value[] values = new Value[additionalValues.length + 2];
        values[0] = Value.create(firstValue);
        values[1] = Value.create(secondValue);
        for (int i = 0; i != additionalValues.length; ++i) {
            values[i + 2] = Value.create(additionalValues[i]);
        }
        return new BasicArray(values);
    }

    static Array create(Iterable<?> values) {
        if (values == null) {
            return create();
        }
        BasicArray array = new BasicArray();
        values.forEach(obj -> array.add(Value.create(obj)));
        return array;
    }

    static Array create(List<Value> values) {
        return (values == null || values.isEmpty()) ? create() : new BasicArray(values);
    }

    /**
     * Return the number of name-value fields in this object.
     *
     * @return the number of name-value fields; never negative
     */
    int size();

    /**
     * Return whether this document contains no fields and is therefore empty.
     *
     * @return true if there are no fields in this document, or false if there is at least one.
     */
    boolean isEmpty();

    /**
     * Determine if this contains an entry at the given index.
     *
     * @param index the index
     * @return true if the entry exists, or false otherwise
     */
    boolean has(int index);

    /**
     * Gets the value in this array at the given index.
     *
     * @param index the index
     * @return The field value, if found, or null otherwise
     */
    Value get(int index);

    /**
     * Gets the value in this document for the given field name.
     *
     * @param index the index
     * @param defaultValue the default value to return if there is no such entry
     * @return The value if found or <code>defaultValue</code> if there is no such entry
     */
    default Value get(int index, Object defaultValue) {
        Value value = get(index);
        return value != null ? value : Value.create(defaultValue);
    }

    /**
     * Determine whether this object has an entry at the given index and the value is null.
     *
     * @param index the index
     * @return <code>true</code> if the entry exists but is null, or false otherwise
     * @see #isNullOrMissing(int)
     */
    default boolean isNull(int index) {
        Value value = get(index);
        return value != null ? value.isNull() : false;
    }

    /**
     * Determine whether this object has an entry at the given index and the value is null, or if this object has no entry at
     * the given index.
     *
     * @param index the index
     * @return <code>true</code> if the field value for the name is null or if there is no such field.
     * @see #isNull(int)
     */
    default boolean isNullOrMissing(int index) {
        Value value = get(index);
        return value != null ? value.isNull() : true;
    }

    /**
     * Remove the specified entry from this array
     *
     * @param index the index
     * @return the value in the removed entry, or null if there is no such entry
     */
    Value remove(int index);

    /**
     * Remove all entries from this array.
     *
     * @return this array to allow for chaining methods
     */
    Array removeAll();

    /**
     * Sets on this object all name/value pairs from the supplied object. If the supplied object is null, this method does
     * nothing.
     *
     * @param values the values to be added to this array
     * @return this array to allow for chaining methods
     */
    default Array addAll(Object... values) {
        if (values != null) {
            for (Object obj : values) {
                add(Value.create(obj));
            }
        }
        return this;
    }

    /**
     * Sets on this object all name/value pairs from the supplied object. If the supplied object is null, this method does
     * nothing.
     *
     * @param values the values to be added to this array
     * @return this array to allow for chaining methods
     */
    default Array addAll(Value... values) {
        if (values != null) {
            addAll(Stream.of(values));
        }
        return this;
    }

    /**
     * Sets on this object all name/value pairs from the supplied object. If the supplied object is null, this method does
     * nothing.
     *
     * @param values the values to be added to this array
     * @return this array to allow for chaining methods
     */
    default Array addAll(Iterable<Value> values) {
        if (values != null) {
            values.forEach(value -> add(value != null ? value.clone() : Value.nullValue()));
        }
        return this;
    }

    /**
     * Sets on this object all name/value pairs from the supplied object. If the supplied object is null, this method does
     * nothing.
     *
     * @param values the values to be added to this array
     * @return this array to allow for chaining methods
     */
    default Array addAll(Stream<Value> values) {
        if (values != null) {
            values.forEach(value -> add(value != null ? value.clone() : Value.nullValue()));
        }
        return this;
    }

    /**
     * Adds the value to the end of this array.
     *
     * @param value the value; may not be null
     * @return this array to allow for chaining methods
     */
    Array add(Value value);

    /**
     * Adds a null value to the end of this array.
     *
     * @return this array to allow for chaining methods
     */
    default Array addNull() {
        add(Value.nullValue());
        return this;
    }

    /**
     * Adds the string value to the end of this array.
     *
     * @param value the string value; may be null if a {@link #addNull() null value} should be added
     * @return this array to allow for chaining methods
     */
    default Array add(String value) {
        add(Value.create(value));
        return this;
    }

    /**
     * Adds the boolean value to the end of this array.
     *
     * @param value the boolean value; may not be null
     * @return this array to allow for chaining methods
     */
    default Array add(boolean value) {
        add(Value.create(value));
        return this;
    }

    /**
     * Adds the boolean value to the end of this array.
     *
     * @param value the boolean value; may be null if a {@link #addNull() null value} should be added
     * @return this array to allow for chaining methods
     */
    default Array add(Boolean value) {
        add(Value.create(value));
        return this;
    }

    /**
     * Adds the integer value to the end of this array.
     *
     * @param value the integer value; may not be null
     * @return this array to allow for chaining methods
     */
    default Array add(int value) {
        add(Value.create(value));
        return this;
    }

    /**
     * Adds the long value to the end of this array.
     *
     * @param value the long value; may not be null
     * @return this array to allow for chaining methods
     */
    default Array add(long value) {
        add(Value.create(value));
        return this;
    }

    /**
     * Adds the float value to the end of this array.
     *
     * @param value the float value; may not be null
     * @return this array to allow for chaining methods
     */
    default Array add(float value) {
        add(Value.create(value));
        return this;
    }

    /**
     * Adds the double value to the end of this array.
     *
     * @param value the double value; may not be null
     * @return this array to allow for chaining methods
     */
    default Array add(double value) {
        add(Value.create(value));
        return this;
    }

    /**
     * Adds the big integer value to the end of this array.
     *
     * @param value the big integer value; may be null if a {@link #addNull() null value} should be added
     * @return this array to allow for chaining methods
     */
    default Array add(BigInteger value) {
        add(Value.create(value));
        return this;
    }

    /**
     * Adds the decimal value to the end of this array.
     *
     * @param value the decimal value; may be null if a {@link #addNull() null value} should be added
     * @return this array to allow for chaining methods
     */
    default Array add(BigDecimal value) {
        add(Value.create(value));
        return this;
    }

    /**
     * Adds the integer value to the end of this array.
     *
     * @param value the integer value; may be null if a {@link #addNull() null value} should be added
     * @return this array to allow for chaining methods
     */
    default Array add(Integer value) {
        add(Value.create(value));
        return this;
    }

    /**
     * Adds the long value to the end of this array.
     *
     * @param value the long value; may be null if a {@link #addNull() null value} should be added
     * @return this array to allow for chaining methods
     */
    default Array add(Long value) {
        add(Value.create(value));
        return this;
    }

    /**
     * Adds the float value to the end of this array.
     *
     * @param value the float value; may be null if a {@link #addNull() null value} should be added
     * @return this array to allow for chaining methods
     */
    default Array add(Float value) {
        add(Value.create(value));
        return this;
    }

    /**
     * Adds the double value to the end of this array.
     *
     * @param value the double value; may be null if a {@link #addNull() null value} should be added
     * @return this array to allow for chaining methods
     */
    default Array add(Double value) {
        add(Value.create(value));
        return this;
    }

    /**
     * Adds the document value to the end of this array.
     *
     * @param value the document value; may be null if a {@link #addNull() null value} should be added
     * @return this array to allow for chaining methods
     */
    default Array add(Document value) {
        add(Value.create(value));
        return this;
    }

    /**
     * Adds the array value to the end of this array.
     *
     * @param value the array value; may be null if a {@link #addNull() null value} should be added
     * @return this array to allow for chaining methods
     */
    default Array add(Array value) {
        add(Value.create(value));
        return this;
    }

    /**
     * Sets on this object all key/value pairs from the supplied map. If the supplied map is null, this method does nothing.
     *
     * @param entries the entries that are to be used to modify this array
     * @return this array to allow for chaining methods
     */
    default Array putAll(Iterable<Entry> entries) {
        if (entries != null) {
            entries.forEach(entry -> {
                if (entry != null) {
                    Value value = entry.getValue().clone();
                    setValue(entry.getIndex(), value);
                }
            });
        }
        return this;
    }

    default Iterable<Value> values() {
        return Iterators.around(Iterators.around(this, (entry) -> entry.getValue()));
    }

    /**
     * Returns a sequential {@code Stream} with this array as its source.
     *
     * @return a sequential {@code Stream} over the elements in this collection
     */
    default Stream<Entry> streamEntries() {
        return StreamSupport.stream(spliterator(), false);
    }

    /**
     * Returns a sequential {@code Stream} with this array as its source.
     *
     * @return a sequential {@code Stream} over the elements in this collection
     */
    default Stream<Value> streamValues() {
        return StreamSupport.stream(values().spliterator(), false);
    }

    /**
     * Transform all of the field values using the supplied {@link BiFunction transformer function}.
     *
     * @param transformer the transformer that should be used to transform each field value; may not be null
     * @return this array with transformed fields, or this document if the transformer changed none of the values
     */
    default Array transform(BiFunction<Integer, Value, Value> transformer) {
        for (int i = 0; i != size(); ++i) {
            Value existing = get(i);
            Value updated = transformer.apply(Integer.valueOf(i), existing);
            if (updated == null) {
                updated = Value.nullValue();
            }
            if (updated != existing) {
                setValue(i, updated);
            }
        }
        return this;
    }

    /**
     * Set the value for the field with the given name to be a null value. The {@link #isNull(int)} methods can be used to
     * determine if a field has been set to null, or {@link #isNullOrMissing(int)} if the field has not be set or if it has
     * been set to null.
     *
     * @param index the index of the field; must be greater than or equal to 0 or less than or equal to {@link #size() size}
     * @return this array to allow for chaining methods
     * @see #isNull(int)
     * @see #isNullOrMissing(int)
     */
    default Array setNull(int index) {
        return setValue(index, Value.nullValue());
    }

    /**
     * Set the value for the field with the given name to the supplied boolean value.
     *
     * @param index the index of the field; must be greater than or equal to 0 or less than or equal to {@link #size() size}
     * @param value the new value for the field
     * @return this array to allow for chaining methods
     */
    default Array setBoolean(int index,
                             boolean value) {
        return setValue(index, Value.create(value));
    }

    /**
     * Set the value for the field with the given name to the supplied integer value.
     *
     * @param index the index of the field; must be greater than or equal to 0 or less than or equal to {@link #size() size}
     * @param value the new value for the field
     * @return this array to allow for chaining methods
     */
    default Array setNumber(int index,
                            int value) {
        return setValue(index, Value.create(value));
    }

    /**
     * Set the value for the field with the given name to the supplied long value.
     *
     * @param index the index of the field; must be greater than or equal to 0 or less than or equal to {@link #size() size}
     * @param value the new value for the field
     * @return this array to allow for chaining methods
     */
    default Array setNumber(int index,
                            long value) {
        return setValue(index, Value.create(value));
    }

    /**
     * Set the value for the field with the given name to the supplied float value.
     *
     * @param index the index of the field; must be greater than or equal to 0 or less than or equal to {@link #size() size}
     * @param value the new value for the field
     * @return this array to allow for chaining methods
     */
    default Array setNumber(int index,
                            float value) {
        return setValue(index, Value.create(value));
    }

    /**
     * Set the value for the field with the given name to the supplied double value.
     *
     * @param index the index of the field; must be greater than or equal to 0 or less than or equal to {@link #size() size}
     * @param value the new value for the field
     * @return this array to allow for chaining methods
     */
    default Array setNumber(int index,
                            double value) {
        return setValue(index, Value.create(value));
    }

    /**
     * Set the value for the field with the given name to the supplied big integer value.
     *
     * @param index the index of the field; must be greater than or equal to 0 or less than or equal to {@link #size() size}
     * @param value the new value for the field
     * @return this array to allow for chaining methods
     */
    default Array setNumber(int index,
                            BigInteger value) {
        return setValue(index, Value.create(value));
    }

    /**
     * Set the value for the field with the given name to the supplied big integer value.
     *
     * @param index the index of the field; must be greater than or equal to 0 or less than or equal to {@link #size() size}
     * @param value the new value for the field
     * @return this array to allow for chaining methods
     */
    default Array setNumber(int index,
                            BigDecimal value) {
        return setValue(index, Value.create(value));
    }

    /**
     * Set the value for the field with the given name to the supplied string value.
     *
     * @param index the index of the field; must be greater than or equal to 0 or less than or equal to {@link #size() size}
     * @param value the new value for the field
     * @return this array to allow for chaining methods
     */
    default Array setString(int index,
                            String value) {
        return setValue(index, Value.create(value));
    }

    /**
     * Set the value for the field with the given name to be a binary value. The value will be encoded as Base64.
     *
     * @param index the index of the field; must be greater than or equal to 0 or less than or equal to {@link #size() size}
     * @param data the bytes for the binary value
     * @return this array to allow for chaining methods
     */
    default Array setBinary(int index,
                            byte[] data) {
        return setValue(index, Value.create(data));
    }

    /**
     * Set the value for the field with the given name to be a value.
     *
     * @param index the index of the field; must be greater than or equal to 0 and less than or equal to {@link #size() size}
     * @param value the new value
     * @return this array to allow for chaining methods
     */
    Array setValue(int index, Value value);

    /**
     * If the current size of the array is smaller than the given size, expand it and use a null value for all new entries.
     * This method does nothing if the current size is larger than the supplied {@code desiredSize}.
     *
     * @param desiredSize the desired size of the array; may be negative
     * @return this array to allow for chaining methods
     */
    default Array expand(int desiredSize) {
        return expand(desiredSize, Value.nullValue());
    }

    /**
     * If the current size of the array is smaller than the given size, expand it and use the supplied value for all new entries.
     * This method does nothing if the current size is larger than the supplied {@code desiredSize}.
     *
     * @param desiredSize the desired size of the array; may be negative
     * @param value  the new value for any new entries
     * @return this array to allow for chaining methods
     */
    Array expand(int desiredSize, Value value);

    /**
     * If the current size of the array is smaller than the given size, expand it and use the supplied value for all new entries.
     * This method does nothing if the current size is larger than the supplied {@code desiredSize}.
     *
     * @param desiredSize the desired size of the array; may be negative
     * @param value  the new value for any new entries
     * @return this array to allow for chaining methods
     */
    default Array expand(int desiredSize, boolean value) {
        return expand(desiredSize, Value.create(value));
    }

    /**
     * If the current size of the array is smaller than the given size, expand it and use the supplied value for all new entries.
     * This method does nothing if the current size is larger than the supplied {@code desiredSize}.
     *
     * @param desiredSize the desired size of the array; may be negative
     * @param value  the new value for any new entries
     * @return this array to allow for chaining methods
     */
    default Array expand(int desiredSize, int value) {
        return expand(desiredSize, Value.create(value));
    }

    /**
     * If the current size of the array is smaller than the given size, expand it and use the supplied value for all new entries.
     * This method does nothing if the current size is larger than the supplied {@code desiredSize}.
     *
     * @param desiredSize the desired size of the array; may be negative
     * @param value  the new value for any new entries
     * @return this array to allow for chaining methods
     */
    default Array expand(int desiredSize, long value) {
        return expand(desiredSize, Value.create(value));
    }

    /**
     * If the current size of the array is smaller than the given size, expand it and use the supplied value for all new entries.
     * This method does nothing if the current size is larger than the supplied {@code desiredSize}.
     *
     * @param desiredSize the desired size of the array; may be negative
     * @param value  the new value for any new entries
     * @return this array to allow for chaining methods
     */
    default Array expand(int desiredSize, float value) {
        return expand(desiredSize, Value.create(value));
    }

    /**
     * If the current size of the array is smaller than the given size, expand it and use the supplied value for all new entries.
     * This method does nothing if the current size is larger than the supplied {@code desiredSize}.
     *
     * @param desiredSize the desired size of the array; may be negative
     * @param value  the new value for any new entries
     * @return this array to allow for chaining methods
     */
    default Array expand(int desiredSize, double value) {
        return expand(desiredSize, Value.create(value));
    }

    /**
     * If the current size of the array is smaller than the given size, expand it and use the supplied value for all new entries.
     * This method does nothing if the current size is larger than the supplied {@code desiredSize}.
     *
     * @param desiredSize the desired size of the array; may be negative
     * @param value  the new value for any new entries
     * @return this array to allow for chaining methods
     */
    default Array expand(int desiredSize, String value) {
        return expand(desiredSize, Value.create(value));
    }

    /**
     * Increment the numeric value at the given location by the designated amount.
     * @param index the index of the field; must be greater than or equal to 0 and less than or equal to {@link #size() size}
     * @param increment the amount to increment the existing value; may be negative to decrement
     * @return this array to allow for chaining methods
     * @throws IllegalArgumentException if the current value is not a number
     */
    default Array increment(int index, int increment) {
        return increment(index, Value.create(increment));
    }

    /**
     * Increment the numeric value at the given location by the designated amount.
     * @param index the index of the field; must be greater than or equal to 0 and less than or equal to {@link #size() size}
     * @param increment the amount to increment the existing value; may be negative to decrement
     * @return this array to allow for chaining methods
     * @throws IllegalArgumentException if the current value is not a number
     */
    default Array increment(int index, long increment) {
        return increment(index, Value.create(increment));
    }

    /**
     * Increment the numeric value at the given location by the designated amount.
     * @param index the index of the field; must be greater than or equal to 0 and less than or equal to {@link #size() size}
     * @param increment the amount to increment the existing value; may be negative to decrement
     * @return this array to allow for chaining methods
     * @throws IllegalArgumentException if the current value is not a number
     */
    default Array increment(int index, double increment) {
        return increment(index, Value.create(increment));
    }

    /**
     * Increment the numeric value at the given location by the designated amount.
     * @param index the index of the field; must be greater than or equal to 0 and less than or equal to {@link #size() size}
     * @param increment the amount to increment the existing value; may be negative to decrement
     * @return this array to allow for chaining methods
     * @throws IllegalArgumentException if the current value is not a number
     */
    default Array increment(int index, float increment) {
        return increment(index, Value.create(increment));
    }

    /**
     * Increment the numeric value at the given location by the designated amount.
     * @param index the index of the field; must be greater than or equal to 0 and less than or equal to {@link #size() size}
     * @param increment the amount to increment the existing value; may be negative to decrement
     * @return this array to allow for chaining methods
     * @throws IllegalArgumentException if the current value is not a number
     */
    Array increment(int index, Value increment);

    /**
     * Set the value for the field with the given name to be a new, empty Document.
     *
     * @param index the index of the field; must be greater than or equal to 0 and less than or equal to {@link #size() size}
     * @return The editable document that was just created; never null
     */
    default Document setDocument(int index) {
        return setDocument(index, Document.create());
    }

    /**
     * Set the value for the field with the given name to be the supplied Document.
     *
     * @param index the index of the field; must be greater than or equal to 0 or less than or equal to {@link #size() size}
     * @param document the document
     * @return The document that was just set as the value for the named field; never null and may or may not be the same
     *         instance as the supplied <code>document</code>.
     */
    default Document setDocument(int index,
                                 Document document) {
        if (document == null) {
            document = Document.create();
        }
        setValue(index, Value.create(document));
        return document;
    }

    /**
     * Set the value for the field with the given name to be a new, empty array.
     *
     * @param index the index of the field; must be greater than or equal to 0 or less than or equal to {@link #size() size}
     * @return The array that was just created; never null
     */
    default Array setArray(int index) {
        return setArray(index, Array.create());
    }

    /**
     * Set the value for the field with the given name to be the supplied array.
     *
     * @param index the index of the field; must be greater than or equal to 0 or less than or equal to {@link #size() size}
     * @param array the array
     * @return The array that was just set as the value for the named field; never null and may or may not be the same
     *         instance as the supplied <code>array</code>.
     */
    default Array setArray(int index,
                           Array array) {
        if (array == null) {
            array = Array.create();
        }
        setValue(index, Value.create(array));
        return array;
    }

    /**
     * Set the value for the field with the given name to be the supplied array.
     *
     * @param index the index of the field; must be greater than or equal to 0 or less than or equal to {@link #size() size}
     * @param values the (valid) values for the array
     * @return The array that was just set as the value for the named field; never null and may or may not be the same
     *         instance as the supplied <code>array</code>.
     */
    default Array setArray(int index,
                           Value... values) {
        Array array = Array.create(values);
        setValue(index, Value.create(array));
        return array;
    }

    /**
     * Obtain a complete copy of this array.
     *
     * @return the clone of this array; never null
     */
    Array clone();

}
