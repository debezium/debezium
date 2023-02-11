/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.schema;

import java.util.List;

/**
 * Common contract for all identifiers of data collections (RDBMS tables, MongoDB collections etc.)
 *
 * @author Gunnar Morling
 */
public interface DataCollectionId {

    /**
     * Get the fully qualified identifier of the data collection.
     *
     * @return the collection's fully qualified identifier.
     */
    String identifier();

    /**
     * Get all elements of the data collection.
     */
    List<String> parts();

    /**
     * Get a database list including database, table name.
     */
    List<String> databaseParts();

    /**
     * Get a schema list including schema, table name.
     */
    List<String> schemaParts();
}
