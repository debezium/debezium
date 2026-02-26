/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.debezium.annotation.NotThreadSafe;

/**
 * A document contains multiple {@link Field}s, each with a name and possibly-null {@link Value}. A single document can only
 * contain a single field with a given name.
 *
 * @author Randall Hauch
 */
@NotThreadSafe
public interface Document extends Iterable<Document.Field>, Comparable<Document> {

    interface Field extends Comparable<Field> {

        /**
         * Get the name of the field
         *
         * @return the field's name; never null
         */
        CharSequence getName();

        /**
         * Get the value of the field.
         *
         * @return the field's value; may be null
         */
        Value getValue();

        default boolean isNull() {
            Value v = getValue();
            return v == null || v.isNull();
        }

        default boolean isNotNull() {
            return !isNull();
        }

        @Override
        default int compareTo(Field that) {
            if (that == null) {
                return 1;
            }
            int diff = this.getName().toString().compareTo(that.getName().toString());
            if (diff != 0) {
                return diff;
            }
            return this.getValue().compareTo(that.getValue());
        }
    }

    static Field field(String name, Value value) {
        return new BasicField(name, value);
    }

    static Field field(String name, Object value) {
        return new BasicField(name, Value.create(value));
    }

    static Document create() {
        return new BasicDocument();
    }

    static Document create(CharSequence fieldName, Object value) {
        return new BasicDocument().set(fieldName, value);
    }

    static Document create(CharSequence fieldName1, Object value1, CharSequence fieldName2, Object value2) {
        return new BasicDocument().set(fieldName1, value1).set(fieldName2, value2);
    }

    static Document create(CharSequence fieldName1, Object value1, CharSequence fieldName2, Object value2, CharSequence fieldName3,
                           Object value3) {
        return new BasicDocument().set(fieldName1, value1).set(fieldName2, value2).set(fieldName3, value3);
    }

    static Document create(CharSequence fieldName1, Object value1, CharSequence fieldName2, Object value2, CharSequence fieldName3,
                           Object value3, CharSequence fieldName4, Object value4) {
        return new BasicDocument().set(fieldName1, value1).set(fieldName2, value2).set(fieldName3, value3).set(fieldName4, value4);
    }

    static Document create(CharSequence fieldName1, Object value1, CharSequence fieldName2, Object value2, CharSequence fieldName3,
                           Object value3, CharSequence fieldName4, Object value4, CharSequence fieldName5, Object value5) {
        return new BasicDocument().set(fieldName1, value1).set(fieldName2, value2).set(fieldName3, value3).set(fieldName4, value4)
                .set(fieldName5, value5);
    }

    static Document create(CharSequence fieldName1, Object value1, CharSequence fieldName2, Object value2, CharSequence fieldName3,
                           Object value3, CharSequence fieldName4, Object value4, CharSequence fieldName5, Object value5,
                           CharSequence fieldName6, Object value6) {
        return new BasicDocument().set(fieldName1, value1).set(fieldName2, value2).set(fieldName3, value3).set(fieldName4, value4)
                .set(fieldName5, value5).set(fieldName6, value6);
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
     * Remove all fields from this document.
     */
    void clear();

    /**
     * Determine if this contains a field with the given name.
     *
     * @param fieldName The name of the field
     * @return true if the field exists, or false otherwise
     */
    boolean has(CharSequence fieldName);

    /**
     * Checks if this object contains all of the fields in the supplied document.
     *
     * @param document The document with the fields that should be in this document
     * @return true if this document contains all of the fields in the supplied document, or false otherwise
     */
    boolean hasAll(Document document);

    /**
     * Set the value at the given path resolved against this document, optionally adding any missing intermediary documents
     * or arrays based upon the format of the path segments.
     *
     * @param path the path at which the value is to be set
     * @param addIntermediaries true if any missing intermediary fields should be created, or false if any missing
     *            intermediary fields should be handled as an error via {@code invalid}
     * @param value the value that should be set at the given path; may be null or a {@link Value#nullValue() null value}
     * @param invalid the function that should be called if the supplied path cannot be resolved; may not be null
     * @return the {@code value} if successful or the {@link Optional#empty() empty (not present)} optional value if
     *         the path was invalid and could not be resolved (and {@code invalid} is invoked)
     */
    default Optional<Value> set(Path path, boolean addIntermediaries, Value value, Consumer<Path> invalid) {
        if (path == null) {
            return Optional.empty();
        }
        if (path.isRoot()) {
            // This is an invalid path, since we don't know what to do with the value given just a root path ...
            invalid.accept(path);
            return Optional.empty();
        }
        if (path.isSingle()) {
            // Perform a simple set ...
            set(path.lastSegment().get(), value);
            return Optional.ofNullable(value);
        }
        // Otherwise, we need to find the parent that will contain the value ...
        Path parentPath = path.parent().get();
        Optional<Value> parent = Optional.empty();
        if (!addIntermediaries) {
            // Any missing intermediaries is considered invalid ...
            parent = find(parentPath, (missingPath, missingIndex) -> {
                invalid.accept(missingPath); // invoke the invalid handler
                return Optional.empty();
            }, invalid);
        }
        else {
            // Create any missing intermediaries using the segment after the missing segment to determine which
            // type of intermediate value to add ...
            parent = find(parentPath, (missingPath, missingIndex) -> {
                String nextSegment = path.segment(missingIndex + 1); // can always find next segment 'path' (not 'parentPath')...
                if (Path.Segments.isArrayIndex(nextSegment)) {
                    return Optional.of(Value.create(Array.create()));
                }
                else {
                    return Optional.of(Value.create(Document.create()));
                }
            }, invalid);
        }
        if (!parent.isPresent()) {
            return Optional.empty();
        }
        String lastSegment = path.lastSegment().get();
        Value parentValue = parent.get();
        if (parentValue.isDocument()) {
            parentValue.asDocument().set(lastSegment, value);
        }
        else if (parentValue.isArray()) {
            Array array = parentValue.asArray();
            if (Path.Segments.isAfterLastIndex(lastSegment)) {
                array.add(value);
            }
            else {
                int index = Path.Segments.asInteger(lastSegment).get();
                array.setValue(index, value);
            }
        }
        else {
            // The parent is not a document or array ...
            invalid.accept(path);
            return Optional.empty();
        }
        return Optional.of(value);
    }

    /**
     * Attempt to find the value at the given path.
     *
     * @param path the path to find
     * @return the optional value at this path, which is {@link Optional#isPresent() present} if the value was found at that
     *         path or is {@link Optional#empty() empty (not present)} if there is no value at the path or if the path was not
     *         valid
     */
    default Optional<Value> find(Path path) {
        return find(path, (missingPath, missingIndex) -> Optional.empty(), (invalidPath) -> {
        });
    }

    /**
     * Attempt to find the value at the given path, optionally creating missing segments.
     *
     * @param path the path to find
     * @param missingSegment function called when a segment in the path does not exist, and which should return a new value
     *            if one should be created or {@link Optional#empty()} if nothing should be created and {@code invalid} function
     *            should be called by this method
     * @param invalid function called when the supplied path is invalid; in this case, this method also returns
     *            {@link Optional#empty()}
     * @return the optional value at this path, which is {@link Optional#isPresent() present} if the value was found at that
     *         path or is {@link Optional#empty() empty (not present)} if there is no value at the path or if the path was not
     *         valid
     */
    default Optional<Value> find(Path path, BiFunction<Path, Integer, Optional<Value>> missingSegment, Consumer<Path> invalid) {
        if (path == null) {
            return Optional.empty();
        }
        if (path.isRoot()) {
            return Optional.of(Value.create(this));
        }
        Value value = Value.create(this);
        int i = 0;
        for (String segment : path) {
            if (value.isDocument()) {
                Value existingValue = value.asDocument().get(segment);
                if (Value.isNull(existingValue)) {
                    // It does not exist ...
                    Optional<Value> newValue = missingSegment.apply(path, i);
                    if (newValue.isPresent()) {
                        // Add the new value (whatever it is) ...
                        Document doc = value.asDocument();
                        doc.set(segment, newValue.get());
                        value = doc.get(segment);
                    }
                    else {
                        return Optional.empty();
                    }
                }
                else {
                    value = existingValue;
                }
            }
            else if (value.isArray()) {
                Array array = value.asArray();
                if (Path.Segments.isAfterLastIndex(segment)) {
                    // This means "after the last index", so call it as missing ...
                    Optional<Value> newValue = missingSegment.apply(path, i);
                    if (newValue.isPresent()) {
                        // Add the new value (whatever it is) ...
                        value = newValue.get();
                        array.add(value);
                    }
                    else {
                        return Optional.empty();
                    }
                }
                else {
                    Optional<Integer> index = Path.Segments.asInteger(segment);
                    if (index.isPresent()) {
                        // This is an index ...
                        if (array.has(index.get())) {
                            value = array.get(index.get());
                        }
                        else if (array.size() == index.get()) {
                            // We can add at this index ...
                            Optional<Value> newValue = missingSegment.apply(path, i);
                            if (newValue.isPresent()) {
                                // Add the new value (whatever it is) ...
                                array.add(newValue.get());
                            }
                            else {
                                return Optional.empty();
                            }
                        }
                        else {
                            // The index is not valid (it's too big to be an existing or the next index) ...
                            invalid.accept(path.subpath(i));
                            return Optional.empty();
                        }
                    }
                    else {
                        // This is not an array index but we're expecting it to be, so this is a bad path
                        invalid.accept(path.subpath(i));
                        return Optional.empty();
                    }
                }
            }
            else {
                // We're supposed to find the segment within this value, but it's not a document or array ...
                invalid.accept(path.subpath(i));
                return Optional.empty();
            }
            ++i;
        }
        return Optional.of(value);
    }

    /**
     * Find a document at the given path and obtain a stream over its fields. This will return an empty stream when:
     * <ul>
     * <li>a value does not exist in this document at the supplied path; or</li>
     * <li>a non-document value does exist in this document at the supplied path; or</li>
     * <li>a document value does exist in this document at the supplied path, but that document is empty</li>
     * </ul>
     *
     * @param path the path to the contained document
     * @return the stream of fields in the document at the given path; never null
     */
    default Stream<Field> children(Path path) {
        Value parent = find(path).orElse(Value.create(Document.create()));
        if (!parent.isDocument()) {
            return Stream.empty();
        }
        return parent.asDocument().stream();
    }

    /**
     * Find the document at the given field name and obtain a stream over its fields. This will return an empty stream when:
     * <ul>
     * <li>a field with the given name does not exist in this document; or</li>
     * <li>a field with the given name does exist in this document but the value is not a document; or</li>
     * <li>a field with the given name does exist in this document and the value is an empty document</li>
     * </ul>
     *
     * @param fieldName the path to the contained document
     * @return the stream of fields within the nested document; never null
     */
    default Stream<Field> children(String fieldName) {
        Document doc = getDocument(fieldName);
        if (doc == null) {
            return Stream.empty();
        }
        return doc.stream();
    }

    /**
     * Gets the field in this document with the given field name.
     *
     * @param fieldName The name of the field
     * @return The field, if found, or null otherwise
     */
    default Field getField(CharSequence fieldName) {
        Value value = get(fieldName);
        return value != null ? new BasicField(fieldName, value) : null;
    }

    /**
     * Gets the value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @return The field value, if found, or null otherwise
     */
    default Value get(CharSequence fieldName) {
        return get(fieldName, null);
    }

    /**
     * Gets the value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field
     * @return The field value, if found, or , or <code>defaultValue</code> if there is no such field
     */
    Value get(CharSequence fieldName, Comparable<?> defaultValue);

    /**
     * Get the boolean value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @return The boolean field value, if found, or null if there is no such field or if the value is not a boolean
     */
    default Boolean getBoolean(CharSequence fieldName) {
        Value value = get(fieldName);
        return value != null && value.isBoolean() ? value.asBoolean() : null;
    }

    /**
     * Get the boolean value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field or if the value is not a boolean
     * @return The boolean field value if found, or <code>defaultValue</code> if there is no such field or if the value is not a
     *         boolean
     */
    default boolean getBoolean(CharSequence fieldName,
                               boolean defaultValue) {
        Value value = get(fieldName);
        return value != null && value.isBoolean() ? value.asBoolean().booleanValue() : defaultValue;
    }

    /**
     * Get the integer value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @return The integer field value, if found, or null if there is no such field or if the value is not an integer
     */
    default Integer getInteger(CharSequence fieldName) {
        Value value = get(fieldName);
        return value != null && value.isInteger() ? value.asInteger() : null;
    }

    /**
     * Get the integer value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field or if the value is not a integer
     * @return The integer field value if found, or <code>defaultValue</code> if there is no such field or if the value is not a
     *         integer
     */
    default int getInteger(CharSequence fieldName,
                           int defaultValue) {
        Value value = get(fieldName);
        return value != null && value.isInteger() ? value.asInteger().intValue() : defaultValue;
    }

    /**
     * Get the integer value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @return The long field value, if found, or null if there is no such field or if the value is not a long value
     */
    default Long getLong(CharSequence fieldName) {
        Value value = get(fieldName);
        return value != null && value.isLong() ? value.asLong() : null;
    }

    /**
     * Get the long value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field or if the value is not a long value
     * @return The long field value if found, or <code>defaultValue</code> if there is no such field or if the value is not a long
     *         value
     */
    default long getLong(CharSequence fieldName,
                         long defaultValue) {
        Value value = get(fieldName);
        return value != null && value.isLong() ? value.asLong().longValue() : defaultValue;
    }

    /**
     * Get the double value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @return The double field value, if found, or null if there is no such field or if the value is not a double
     */
    default Double getDouble(CharSequence fieldName) {
        Value value = get(fieldName);
        return value != null && value.isDouble() ? value.asDouble() : null;
    }

    /**
     * Get the double value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field or if the value is not a double
     * @return The double field value if found, or <code>defaultValue</code> if there is no such field or if the value is not a
     *         double
     */
    default double getDouble(CharSequence fieldName,
                             double defaultValue) {
        Value value = get(fieldName);
        return value != null && value.isDouble() ? value.asDouble().doubleValue() : defaultValue;
    }

    /**
     * Get the double value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @return The double field value, if found, or null if there is no such field or if the value is not a double
     */
    default Float getFloat(CharSequence fieldName) {
        Value value = get(fieldName);
        return value != null && value.isFloat() ? value.asFloat() : null;
    }

    /**
     * Get the float value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field or if the value is not a double
     * @return The double field value if found, or <code>defaultValue</code> if there is no such field or if the value is not a
     *         double
     */
    default float getFloat(CharSequence fieldName,
                           float defaultValue) {
        Value value = get(fieldName);
        return value != null && value.isFloat() ? value.asFloat().floatValue() : defaultValue;
    }

    /**
     * Get the number value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @return The number field value, if found, or null if there is no such field or if the value is not a number
     */
    default Number getNumber(CharSequence fieldName) {
        return getNumber(fieldName, null);
    }

    /**
     * Get the number value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field or if the value is not a number
     * @return The number field value if found, or <code>defaultValue</code> if there is no such field or if the value is not a
     *         number
     */
    default Number getNumber(CharSequence fieldName,
                             Number defaultValue) {
        Value value = get(fieldName);
        return value != null && value.isNumber() ? value.asNumber() : defaultValue;
    }

    /**
     * Get the big integer value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @return The big integer field value, if found, or null if there is no such field or if the value is not a big integer
     */
    default BigInteger getBigInteger(CharSequence fieldName) {
        return getBigInteger(fieldName, null);
    }

    /**
     * Get the big integer value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field or if the value is not a big integer
     * @return The big integer field value, if found, or null if there is no such field or if the value is not a big integer
     */
    default BigInteger getBigInteger(CharSequence fieldName, BigInteger defaultValue) {
        Value value = get(fieldName);
        return value != null && value.isBigInteger() ? value.asBigInteger() : defaultValue;
    }

    /**
     * Get the big decimal value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @return The big decimal field value, if found, or null if there is no such field or if the value is not a big decimal
     */
    default BigDecimal getBigDecimal(CharSequence fieldName) {
        return getBigDecimal(fieldName, null);
    }

    /**
     * Get the big decimal value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field or if the value is not a big decimal
     * @return The big decimal field value, if found, or null if there is no such field or if the value is not a big decimal
     */
    default BigDecimal getBigDecimal(CharSequence fieldName, BigDecimal defaultValue) {
        Value value = get(fieldName);
        return value != null && value.isBigDecimal() ? value.asBigDecimal() : defaultValue;
    }

    /**
     * Get the string value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @return The string field value, if found, or null if there is no such field or if the value is not a string
     */
    default String getString(CharSequence fieldName) {
        return getString(fieldName, null);
    }

    /**
     * Get the string value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field or if the value is not a string
     * @return The string field value if found, or <code>defaultValue</code> if there is no such field or if the value is not a
     *         string
     */
    default String getString(CharSequence fieldName,
                             String defaultValue) {
        Value value = get(fieldName);
        return value != null && value.isString() ? value.asString() : defaultValue;
    }

    /**
     * Get the Base64 encoded binary value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @return The binary field value, if found, or null if there is no such field or if the value is not a binary value
     */
    default byte[] getBytes(CharSequence fieldName) {
        Value value = get(fieldName);
        return value != null && value.isBinary() ? value.asBytes() : null;
    }

    /**
     * Get the array value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @return The array field value (as a list), if found, or null if there is no such field or if the value is not an array
     */
    default Array getArray(CharSequence fieldName) {
        return getArray(fieldName, null);
    }

    /**
     * Get the array value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @param defaultValue the default array that should be returned if there is no such field
     * @return The array field value (as a list), if found, or the default value if there is no such field or if the value is not
     *         an array
     */
    default Array getArray(CharSequence fieldName, Array defaultValue) {
        Value value = get(fieldName);
        return value != null && value.isArray() ? value.asArray() : defaultValue;
    }

    /**
     * Get the existing array value in this document for the given field name, or create a new array if there is no existing array
     * at this field.
     *
     * @param fieldName The name of the field
     * @return The editable array field value; never null
     */
    default Array getOrCreateArray(CharSequence fieldName) {
        Value value = get(fieldName);
        if (value == null || value.isNull()) {
            return setArray(fieldName, (Array) null);
        }
        return value.asArray();
    }

    /**
     * Get the document value in this document for the given field name.
     *
     * @param fieldName The name of the field
     * @return The document field value, if found, or null if there is no such field or if the value is not a document
     */
    default Document getDocument(CharSequence fieldName) {
        Value value = get(fieldName);
        return value != null && value.isDocument() ? value.asDocument() : null;
    }

    /**
     * Get the existing document value in this document for the given field name, or create a new document if there is no existing
     * document at this field.
     *
     * @param fieldName The name of the field
     * @return The editable document field value; null if the field exists but is not a document
     */
    default Document getOrCreateDocument(CharSequence fieldName) {
        Value value = get(fieldName);
        if (value == null || value.isNull()) {
            return setDocument(fieldName, (Document) null);
        }
        return value.asDocument();
    }

    /**
     * Determine whether this object has a field with the given the name and the value is null. This is equivalent to calling:
     *
     * <pre>
     * this.get(name) instanceof Null;
     * </pre>
     *
     * @param fieldName The name of the field
     * @return <code>true</code> if the field exists but is null, or false otherwise
     * @see #isNullOrMissing(CharSequence)
     */
    default boolean isNull(CharSequence fieldName) {
        Value value = get(fieldName);
        return value != null && value.isNull();
    }

    /**
     * Determine whether this object has a field with the given the name and the value is null, or if this object has no field
     * with
     * the given name. This is equivalent to calling:
     *
     * <pre>
     * Null.matches(this.get(name));
     * </pre>
     *
     * @param fieldName The name of the field
     * @return <code>true</code> if the field value for the name is null or if there is no such field.
     * @see #isNull(CharSequence)
     */
    default boolean isNullOrMissing(CharSequence fieldName) {
        Value value = get(fieldName);
        return value == null || value.isNull();
    }

    /**
     * Returns this object's fields' names
     *
     * @return The names of the fields in this object
     */
    Iterable<CharSequence> keySet();

    /**
     * Obtain a clone of this document.
     *
     * @return the clone of this document; never null
     */
    Document clone();

    /**
     * Remove the field with the supplied name, and return the value.
     *
     * @param name The name of the field
     * @return the value that was removed, or null if there was no such value
     */
    Value remove(CharSequence name);

    /**
     * If the supplied name is provided, then remove the field with the supplied name and return the value.
     *
     * @param name The optional name of the field
     * @return the value that was removed, or null if the field was not present or there was no such value
     */
    default Value remove(Optional<? extends CharSequence> name) {
        return name.isPresent() ? remove(name.get()) : null;
    }

    /**
     * Remove all fields from this document.
     *
     * @return This document, to allow for chaining methods
     */
    Document removeAll();

    /**
     * Sets on this object all name/value pairs from the supplied object. If the supplied object is null, this method does
     * nothing.
     *
     * @param fields the name/value pairs to be set on this object; may not be null
     * @return This document, to allow for chaining methods
     */
    default Document putAll(Iterator<Field> fields) {
        while (fields.hasNext()) {
            Field field = fields.next();
            setValue(field.getName(), field.getValue());
        }
        return this;
    }

    /**
     * Sets on this object all name/value pairs from the supplied object. If the supplied object is null, this method does
     * nothing.
     *
     * @param fields the name/value pairs to be set on this object; may not be null
     * @return This document, to allow for chaining methods
     */
    default Document putAll(Iterable<Field> fields) {
        for (Field field : fields) {
            setValue(field.getName(), field.getValue());
        }
        return this;
    }

    /**
     * Attempts to copy all of the acceptable fields from the source and set on this document, overwriting any existing
     * values.
     *
     * @param fields the name/value pairs to be set on this object; may not be null
     * @param acceptableFieldNames the predicate to determine which fields from the source should be copied; may not be null
     * @return This document, to allow for chaining methods
     */
    default Document putAll(Iterable<Field> fields, Predicate<CharSequence> acceptableFieldNames) {
        for (Field field : fields) {
            if (acceptableFieldNames.test(field.getName())) {
                setValue(field.getName(), field.getValue());
            }
        }
        return this;
    }

    /**
     * Sets on this object all key/value pairs from the supplied map. If the supplied map is null, this method does nothing.
     *
     * @param fields the map containing the name/value pairs to be set on this object
     * @return This document, to allow for chaining methods
     */
    default Document putAll(Map<? extends CharSequence, ?> fields) {
        if (fields != null) {
            for (Map.Entry<? extends CharSequence, ?> entry : fields.entrySet()) {
                set(entry.getKey(), entry.getValue());
            }
        }
        return this;
    }

    /**
     * Returns a sequential {@code Stream} with this array as its source.
     *
     * @return a sequential {@code Stream} over the elements in this collection
     */
    default Stream<Field> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    default void forEach(BiConsumer<Path, Value> consumer) {
        Path root = Path.root();
        stream().forEach((field) -> {
            Path path = root.append(field.getName().toString());
            Value value = field.getValue();
            if (value.isDocument()) {
                value.asDocument().forEach((p, v) -> {
                    consumer.accept(path.append(p), v);
                });
            }
            else if (value.isArray()) {
                value.asArray().forEach((entry) -> {
                    consumer.accept(path.append(Integer.toString(entry.getIndex())), entry.getValue());
                });
            }
            else {
                consumer.accept(path, value);
            }
        });
    }

    /**
     * Transform all of the field values using the supplied {@link BiFunction transformer function}.
     *
     * @param transformer the transformer that should be used to transform each field value; may not be null
     * @return this document with transformed fields, or this document if the transformer changed none of the values
     */
    default Document transform(BiFunction<CharSequence, Value, Value> transformer) {
        for (Field field : this) {
            Value existing = get(field.getName());
            Value updated = transformer.apply(field.getName(), existing);
            if (updated == null) {
                updated = Value.nullValue();
            }
            if (updated != existing) {
                setValue(field.getName(), updated);
            }
        }
        return this;
    }

    /**
     * Set the value for the field with the given name to be a binary value. The value will be encoded as Base64.
     *
     * @param name The name of the field
     * @param value the new value
     * @return This document, to allow for chaining methods
     */
    default Document set(CharSequence name, Object value) {
        if (value instanceof Value) {
            setValue(name, (Value) value);
            return this;
        }
        Value wrapped = Value.create(value);
        setValue(name, wrapped);
        return this;

    }

    /**
     * Set the value for the field with the given name to be a null value. The {@link #isNull(CharSequence)} methods can be used
     * to
     * determine if a field has been set to null, or {@link #isNullOrMissing(CharSequence)} if the field has not be set or if it
     * has
     * been set to null.
     *
     * @param name The name of the field
     * @return This document, to allow for chaining methods
     * @see #isNull(CharSequence)
     * @see #isNullOrMissing(CharSequence)
     */
    default Document setNull(CharSequence name) {
        setValue(name, Value.nullValue());
        return this;
    }

    /**
     * Set the value for the field with the given name to the supplied boolean value.
     *
     * @param name The name of the field
     * @param value the new value for the field
     * @return This document, to allow for chaining methods
     */
    default Document setBoolean(CharSequence name,
                                boolean value) {
        setValue(name, Value.create(value));
        return this;
    }

    /**
     * Set the value for the field with the given name to the supplied integer value.
     *
     * @param name The name of the field
     * @param value the new value for the field
     * @return This document, to allow for chaining methods
     */
    default Document setNumber(CharSequence name,
                               int value) {
        setValue(name, Value.create(value));
        return this;
    }

    /**
     * Set the value for the field with the given name to the supplied long value.
     *
     * @param name The name of the field
     * @param value the new value for the field
     * @return This document, to allow for chaining methods
     */
    default Document setNumber(CharSequence name,
                               long value) {
        setValue(name, Value.create(value));
        return this;
    }

    /**
     * Set the value for the field with the given name to the supplied float value.
     *
     * @param name The name of the field
     * @param value the new value for the field
     * @return This document, to allow for chaining methods
     */
    default Document setNumber(CharSequence name,
                               float value) {
        setValue(name, Value.create(value));
        return this;
    }

    /**
     * Set the value for the field with the given name to the supplied double value.
     *
     * @param name The name of the field
     * @param value the new value for the field
     * @return This document, to allow for chaining methods
     */
    default Document setNumber(CharSequence name,
                               double value) {
        setValue(name, Value.create(value));
        return this;
    }

    /**
     * Set the value for the field with the given name to the supplied big integer value.
     *
     * @param name The name of the field
     * @param value the new value for the field
     * @return This document, to allow for chaining methods
     */
    default Document setNumber(CharSequence name,
                               BigInteger value) {
        setValue(name, Value.create(value));
        return this;
    }

    /**
     * Set the value for the field with the given name to the supplied big integer value.
     *
     * @param name The name of the field
     * @param value the new value for the field
     * @return This document, to allow for chaining methods
     */
    default Document setNumber(CharSequence name,
                               BigDecimal value) {
        setValue(name, Value.create(value));
        return this;
    }

    /**
     * Set the value for the field with the given name to the supplied string value.
     *
     * @param name The name of the field
     * @param value the new value for the field
     * @return This document, to allow for chaining methods
     */
    default Document setString(CharSequence name,
                               String value) {
        setValue(name, Value.create(value));
        return this;
    }

    /**
     * Increment the numeric value in the given field by the designated amount.
     *
     * @param name The name of the field
     * @param increment the amount to increment the existing value; may be negative to decrement
     * @return this array to allow for chaining methods
     * @throws IllegalArgumentException if the current value is not a number
     */
    default Document increment(CharSequence name, int increment) {
        return increment(name, Value.create(increment));
    }

    /**
     * Increment the numeric value in the given field by the designated amount.
     *
     * @param name The name of the field
     * @param increment the amount to increment the existing value; may be negative to decrement
     * @return this array to allow for chaining methods
     * @throws IllegalArgumentException if the current value is not a number
     */
    default Document increment(CharSequence name, long increment) {
        return increment(name, Value.create(increment));
    }

    /**
     * Increment the numeric value in the given field by the designated amount.
     *
     * @param name The name of the field
     * @param increment the amount to increment the existing value; may be negative to decrement
     * @return this array to allow for chaining methods
     * @throws IllegalArgumentException if the current value is not a number
     */
    default Document increment(CharSequence name, double increment) {
        return increment(name, Value.create(increment));
    }

    /**
     * Increment the numeric value in the given field by the designated amount.
     *
     * @param name The name of the field
     * @param increment the amount to increment the existing value; may be negative to decrement
     * @return this array to allow for chaining methods
     * @throws IllegalArgumentException if the current value is not a number
     */
    default Document increment(CharSequence name, float increment) {
        return increment(name, Value.create(increment));
    }

    /**
     * Increment the numeric value in the given field by the designated amount.
     *
     * @param name The name of the field
     * @param increment the amount to increment the existing value; may be negative to decrement
     * @return this array to allow for chaining methods
     * @throws IllegalArgumentException if the current value is not a number
     */
    Document increment(CharSequence name, Value increment);

    /**
     * Set the value for the field with the given name to be a binary value. The value will be encoded as Base64.
     *
     * @param name The name of the field
     * @param data the bytes for the binary value
     * @return This document, to allow for chaining methods
     */
    default Document setBinary(CharSequence name,
                               byte[] data) {
        setValue(name, Value.create(data));
        return this;
    }

    /**
     * Set the value for the field with the given name.
     *
     * @param name The name of the field
     * @param value the new value
     * @return This document, to allow for chaining methods
     */
    Document setValue(CharSequence name,
                      Value value);

    /**
     * Set the field on this document.
     *
     * @param field The field
     * @return This document, to allow for chaining methods
     */
    default Document setValue(Field field) {
        return setValue(field.getName(), field.getValue());
    }

    /**
     * Set the value for the field with the given name to be a new, empty Document.
     *
     * @param name The name of the field
     * @return The editable document that was just created; never null
     */
    default Document setDocument(CharSequence name) {
        return setDocument(name, Document.create());
    }

    /**
     * Set the value for the field with the given name to be the supplied Document.
     *
     * @param name The name of the field
     * @param document the document; if null, a new document will be created
     * @return The document that was just set as the value for the named field; never null and may or may not be the same
     *         instance as the supplied <code>document</code>.
     */
    default Document setDocument(CharSequence name,
                                 Document document) {
        if (document == null) {
            document = Document.create();
        }
        setValue(name, Value.create(document));
        return getDocument(name);
    }

    /**
     * Set the value for the field with the given name to be a new, empty array.
     *
     * @param name The name of the field
     * @return The array that was just created; never null
     */
    default Array setArray(CharSequence name) {
        return setArray(name, Array.create());
    }

    /**
     * Set the value for the field with the given name to be the supplied array.
     *
     * @param name The name of the field
     * @param array the array
     * @return The array that was just set as the value for the named field; never null and may or may not be the same
     *         instance as the supplied <code>array</code>.
     */
    default Array setArray(CharSequence name,
                           Array array) {
        if (array == null) {
            array = Array.create();
        }
        setValue(name, Value.create(array));
        return getArray(name);
    }

    /**
     * Set the value for the field with the given name to be the supplied array.
     *
     * @param name The name of the field
     * @param values the (valid) values for the array
     * @return The array that was just set as the value for the named field; never null and may or may not be the same
     *         instance as the supplied <code>array</code>.
     */
    default Array setArray(CharSequence name,
                           Object... values) {
        return setArray(name, Array.create(values));
    }

    /**
     * Compare this Document to the specified Document, taking into account the order of the fields.
     *
     * @param that the other Document to be compared to this object
     * @return a negative integer, zero, or a positive integer as this object
     *         is less than, equal to, or greater than the specified object.
     */
    @Override
    int compareTo(Document that);

    /**
     * Compare this Document to the specified Document, without regard to the order of the fields.
     *
     * @param that the other Document to be compared to this object
     * @return a negative integer, zero, or a positive integer as this object
     *         is less than, equal to, or greater than the specified object.
     */
    int compareToWithoutFieldOrder(Document that);

    /**
     * Compare this Document to the specified Document, without regard to the order of the fields and only using the fields
     * that are in both documents.
     *
     * @param that the other Document to be compared to this object
     * @return a negative integer, zero, or a positive integer as this object
     *         is less than, equal to, or greater than the specified object.
     */
    int compareToUsingSimilarFields(Document that);

    /**
     * Compare this Document to the specified Document, optionally comparing the fields in the same order.
     *
     * @param that the other Document to be compared to this object
     * @param enforceFieldOrder {@code true} if the documents should be compared using their existing field order, or
     *            {@code false} if the field order should not affect the result.
     * @return a negative integer, zero, or a positive integer as this object
     *         is less than, equal to, or greater than the specified object.
     */
    int compareTo(Document that, boolean enforceFieldOrder);
}
