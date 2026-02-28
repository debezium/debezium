/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metadata;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.Field;

/**
 * Interface for components (connectors, transforms, converters) to expose their configuration fields.
 * This provides a consistent, explicit way to retrieve configuration metadata without relying on reflection.
 */
@Incubating
public interface ConfigDescriptor {

    /**
     * Returns the set of configuration fields supported by this component.
     *
     * @return Field.Set containing all configuration fields
     */
    Field.Set getConfigFields();
}
