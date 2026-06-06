/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

/**
 * Invoked to convert incoming SQL column values into Kafka Connect values. The callback approach is used in order to
 * tell apart the case where a conversion returned {@code null} from it returning no value at all (because the incoming
 * value type is unsupported):
 *
 * @author Gunnar Morling
 */
@FunctionalInterface
public interface ValueConversionCallback {

    void convert(ResultReceiver receiver);
}
