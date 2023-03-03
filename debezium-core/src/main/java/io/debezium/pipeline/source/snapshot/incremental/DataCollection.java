/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.util.Objects;
import java.util.Optional;

/**
 * A class describing DataCollection for incremental snapshot
 *
 * @author Vivek Wassan
 *
 */
public class DataCollection<T> {

    private final T id;

    private final Optional<String> additionalCondition;

    private final Optional<String> surrogateKey;

    public DataCollection(T id) {
        this(id, Optional.empty(), Optional.empty());
    }

    public DataCollection(T id, Optional<String> additionalCondition, Optional<String> surrogateKey) {
        Objects.requireNonNull(additionalCondition);
        Objects.requireNonNull(surrogateKey);

        this.id = id;
        this.additionalCondition = additionalCondition;
        this.surrogateKey = surrogateKey;
    }

    public T getId() {
        return id;
    }

    public Optional<String> getAdditionalCondition() {
        return additionalCondition;
    }

    public Optional<String> getSurrogateKey() {
        return surrogateKey;
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
        return "DataCollection{" +
                "id=" + id +
                ", additionalCondition=" + additionalCondition +
                ", surrogateKey=" + surrogateKey +
                '}';
    }
}
