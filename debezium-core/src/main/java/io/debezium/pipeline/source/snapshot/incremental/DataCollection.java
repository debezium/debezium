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

    private T id;

    private Optional<String> additionalCondition;

    public DataCollection(T id, Optional<String> additionalCondition) {
        this.id = id;
        this.additionalCondition = additionalCondition == null ? Optional.empty() : additionalCondition;
    }

    public T getId() {
        return id;
    }

    public void setId(T id) {
        this.id = id;
    }

    public Optional<String> getAdditionalCondition() {
        return additionalCondition;
    }

    public void setAdditionalCondition(Optional<String> additionalCondition) {
        this.additionalCondition = additionalCondition;
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
                '}';
    }
}
