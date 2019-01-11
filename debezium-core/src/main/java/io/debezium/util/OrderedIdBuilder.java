/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import org.apache.kafka.connect.data.Schema;

public abstract class OrderedIdBuilder {
    /**
     * Indicates if this id builder should include an id
     * @return true if it should include, otherwise false
     */
    public abstract Boolean shouldIncludeId();

    /**
     * builds the next id
     * @return the next id
     */
    public abstract String buildNextId();

    /**
     * The last id that was generated
     * @return the last id generated
     */
    public abstract String lastId();

    /**
     * restores to a previous id
     * @param state the id to restore to
     */
    public abstract void restoreState(String state);

    /**
     * clones this id builder along with state.
     *
     * This is primarily used for connectors to track multiple
     * sources simultaneously
     * @return a new id builder with previous state
     */
    public abstract OrderedIdBuilder clone();

    public static Schema schema() {
        return Schema.OPTIONAL_STRING_SCHEMA;
    }
}
