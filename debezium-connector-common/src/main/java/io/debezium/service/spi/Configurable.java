/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service.spi;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;

/**
 * Allows the service to request the connector configuration.
 *
 * A service can be registered either using a {@link ServiceProvider} or directly to the registry, and
 * for the former, you can consider using the provider to configure the service; however, this allows
 * for avoiding constructor argument bloat by configuring the service inline if needed.
 *
 * @author Chris Cranford
 */
@Incubating
public interface Configurable {
    /**
     * Configure the service.
     *
     * @param configuration the connector configuration
     */
    void configure(Configuration configuration);
}
