/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.spi;

import io.debezium.converters.spi.RecordParser;

/**
 * A {@link java.util.ServiceLoader} interface that connectors should implement to resolve fully qualified table name.
 *
 * @author Mario Fiore Vitale
 */
public interface QualifiedDataCollectionNameResolver extends RecordParserProvider {

    /**
     * Resolve the fully qualified table name.
     *
     * @return resolved fully qualified table name
     */

    String resolve(RecordParser recordParser);
}
