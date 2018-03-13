/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive.converters;

import io.debezium.reactive.spi.AsType;

public class AsJson implements AsType<String> {

    public Class<String> getTargetType() {
        return String.class;
    }
}
