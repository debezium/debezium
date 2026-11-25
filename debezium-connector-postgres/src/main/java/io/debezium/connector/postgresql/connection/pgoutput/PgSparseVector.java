/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import java.util.Map;

/**
 * A record to represent sparsevec.
 * @param dimensions total number of dimensions
 * @param vector non-zero index-value map
 */
public record PgSparseVector(int dimensions, Map<Integer, Double> vector) {
}
