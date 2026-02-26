/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.time.Duration;

public final class ConfigurationDefaults {
    /**
     * The maximum wait time before {@code poll()} return control back to Connect when no events
     * are available.
     */
    public static final Duration RETURN_CONTROL_INTERVAL = Duration.ofSeconds(5);
}
