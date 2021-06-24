/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.converter;

import io.debezium.common.annotation.Incubating;

/**
 * An the base interface for a converted field that provides naming characteristics.
 *
 * @author Jiri Pechanec
 */
@Incubating
public interface ConvertedField {

    /**
     * @return the name of the data field in data collection (like column in a table)
     */
    String name();

    /**
     * @return the qualified name of the data collection (like a table)
     */
    String dataCollection();
}
