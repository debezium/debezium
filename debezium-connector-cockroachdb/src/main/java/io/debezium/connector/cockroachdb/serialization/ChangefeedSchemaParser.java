/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb.serialization;

import org.apache.kafka.connect.data.Schema;

/**
 * Interface for parsing CockroachDB changefeed messages into Debezium-compatible
 * key/value schema and data pairs.
 * <p>
 * Implementations may handle JSON, Avro, or other formats.
 *
 * @author Virag Tripathi
 */
public interface ChangefeedSchemaParser {

    record ParsedChange(
            Schema keySchema,
            Object key,
            Schema valueSchema,
            Object value) {
    }

    ParsedChange parse(String keyJson, String valueJson) throws Exception;
}
