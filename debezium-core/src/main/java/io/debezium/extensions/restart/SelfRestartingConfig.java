/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.extensions.restart;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.Field;

/**
 * The configuration properties.
 */
public class SelfRestartingConfig {
    private static final long MAXIMUM_TIMEOUT_IN_MS = TimeUnit.MINUTES.toMillis(5);
    private static final long INITIAL_BACK_OFF_PAUSE_IN_MS = TimeUnit.SECONDS.toMillis(2);
    private static final long MAXIMUM_BACK_OFF_PAUSE_IN_MS = TimeUnit.MINUTES.toMillis(1);

    public static final Field ENABLED = Field.create("extension.restart.enabled")
            .withDisplayName("Restart enabled")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withValidation(Field::isBoolean)
            .withDescription("If set to true, the connector will try restart on error");

    public static final Field MAX_TIMEOUT = Field.create("extension.restart.timeout.maximum")
            .withDisplayName("Maximum restart timeout")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(MAXIMUM_TIMEOUT_IN_MS)
            .withValidation(Field::isInteger)
            .withDescription("For how long the connector tries restarting in case of failure (in ms)");

    public static final Field BACK_OFF_INITIAL = Field.create("extension.restart.backoff.exponential.initial")
            .withDisplayName("Initial pause between restarts")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(INITIAL_BACK_OFF_PAUSE_IN_MS)
            .withValidation(Field::isInteger)
            .withDescription("Initial time between restart attempts (in ms)");

    public static final Field BACK_OFF_MAXIMUM = Field.create("extension.restart.backoff.exponential.maximum")
            .withDisplayName("Maximum pause between restarts")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(MAXIMUM_BACK_OFF_PAUSE_IN_MS)
            .withValidation(Field::isInteger)
            .withDescription("Maximum time between restart attempts (in ms)");

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(ENABLED, MAX_TIMEOUT, BACK_OFF_INITIAL, BACK_OFF_MAXIMUM);

    public static ConfigDef configDef(final ConfigDef config) {
        Field.group(config, "Restarting", ENABLED, MAX_TIMEOUT, BACK_OFF_INITIAL, BACK_OFF_MAXIMUM);
        return config;
    }
}
