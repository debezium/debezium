/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.util.Objects;
import java.util.Optional;

import io.debezium.util.Strings;

/**
 * A class describing CollectionId for incremental snapshot
 *
 * @author Vivek Wassan
 *
 */
public class DataCollection<T> {

    private final T id;

    private final String additionalCondition;

    private final String surrogateKey;

    /**
     * The id of the signal that requested snapshotting of this data collection, if any.
     * Kept per data collection because collections requested by different signals can be
     * queued in the same incremental snapshot context.
     */
    private final String correlationId;

    public DataCollection(T id) {
        this(id, "", "", null);
    }

    public DataCollection(T id, String additionalCondition, String surrogateKey) {
        this(id, additionalCondition, surrogateKey, null);
    }

    public DataCollection(T id, String additionalCondition, String surrogateKey, String correlationId) {
        Objects.requireNonNull(additionalCondition);
        Objects.requireNonNull(surrogateKey);

        this.id = id;
        this.additionalCondition = additionalCondition;
        this.surrogateKey = surrogateKey;
        this.correlationId = correlationId;
    }

    public T getId() {
        return id;
    }

    public Optional<String> getAdditionalCondition() {
        // Encapsulate additional condition into parenthesis to make sure its own logical operators
        // do not interfere with the built query
        return Strings.isNullOrEmpty(additionalCondition) ? Optional.empty() : Optional.of("(" + additionalCondition + ")");
    }

    public Optional<String> getSurrogateKey() {
        return Strings.isNullOrEmpty(surrogateKey) ? Optional.empty() : Optional.of(surrogateKey);
    }

    public Optional<String> getCorrelationId() {
        return Strings.isNullOrEmpty(correlationId) ? Optional.empty() : Optional.of(correlationId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataCollection<?> that = (DataCollection<?>) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "CollectionId{" +
                "id=" + id +
                ", additionalCondition=" + additionalCondition +
                ", surrogateKey=" + surrogateKey +
                ", correlationId=" + correlationId +
                '}';
    }
}
