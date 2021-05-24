/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.util.ArrayList;
import java.util.List;

/**
 * A "marker" class for passing a collection of Blob data type chunks to {@link OracleValueConverters}
 * so that each chunk can be converted, decoded, and combined into a single binary representation
 * for event emission.
 *
 * @author Chris Cranford
 */
public class BlobChunkList extends ArrayList<String> {
    /**
     * Creates a BLOB chunk list backed by the provided collection.
     *
     * @param backingList collection of BLOB chunks
     */
    public BlobChunkList(List<String> backingList) {
        super(backingList);
    }
}
