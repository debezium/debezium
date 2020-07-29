/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.format;

import io.debezium.common.annotation.Incubating;

/**
 * A {@link SerializationFormat} defining the Protobuf format serialized as byte[].
 */
@Incubating
public class Protobuf implements SerializationFormat<byte[]> {
}
