/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import org.apache.kafka.connect.source.SourceRecord;

/**
 * A predicate invoked by {@link Reader} implementations in order to determine whether they should continue with
 * processing records or not.
 *
 * @author Gunnar Morling
 */
@FunctionalInterface
public interface HaltingPredicate {

    /**
     * Whether this record should be processed by the calling reader or not.
     */
    boolean accepts(SourceRecord record);
}
