/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import java.util.Objects;

import io.debezium.annotation.Immutable;

/**
 * Package-level implementation of {@link Array.Entry} in an {@link Array}.
 *
 * @author Randall Hauch
 */
@Immutable
final class BasicEntry implements Array.Entry, Comparable<Array.Entry> {

    private final int index;
    private final Value value;

    BasicEntry(int index, Value value) {
        this.index = index;
        this.value = value;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public Value getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "@" + index + "=" + value;
    }

    @Override
    public int hashCode() {
        return index;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof Array.Entry) {
            Array.Entry that = (Array.Entry) obj;
            return this.getIndex() == that.getIndex() && Objects.equals(this.getValue(), that.getValue());
        }
        return false;
    }

    @Override
    public int compareTo(Array.Entry that) {
        if (this == that) {
            return 0;
        }
        if (this.getIndex() != that.getIndex()) {
            return this.getIndex() - that.getIndex();
        }
        return Value.compareTo(this.getValue(), that.getValue());
    }
}
