/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

/**
 * A configuration option with a fixed set of possible values, i.e. an enum. To be implemented by any enum
 * types used with {@link ConfigBuilder}.
 *
 * @author Brendan Maguire
 */
public interface EnumeratedValue {

    /**
     * Returns the string representation of this value
     * @return The string representation of this value
     */
    String getValue();
}
