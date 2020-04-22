/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.format;

import io.debezium.engine.ChangeEventFormat;
import io.debezium.engine.KeyValueChangeEventFormat;

/**
 * A {@link ChangeEventFormat} defining the JSON format serialized as String.
 */
public class CloudEvents implements KeyValueChangeEventFormat<String> {
}
