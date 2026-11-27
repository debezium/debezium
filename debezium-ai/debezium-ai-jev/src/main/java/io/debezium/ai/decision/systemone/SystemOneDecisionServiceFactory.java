/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.systemone;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.debezium.ai.decision.api.DecisionService;
import io.debezium.ai.decision.api.Question;
import io.debezium.ai.decision.spi.DecisionServiceFactory;
import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Shared configuration and service creation for System One providers.
 *
 * @author Jiri Pechanec
 */
public abstract class SystemOneDecisionServiceFactory implements DecisionServiceFactory {

    private final String name;
    private final String displayName;
    private final boolean keyRequired;
    private final int maxScoreLevels;
    private final Field apiKey;
    private final Field baseUrlField;
    private final Field modelField;
    private final Field longContext;
    private final Field requestTimeoutMs;
    private final Field connectTimeoutMs;
    private final Field retryMaxAttempts;
    private final Field retryInitialDelayMs;
    private final Field retryMaxDelayMs;
    private final Field.Set fields;

    protected SystemOneDecisionServiceFactory(final String name, final String displayName, final String baseUrl, final String model,
                                              final boolean keyRequired, final boolean longContextSupported, final int maxScoreLevels) {
        this.name = name;
        this.displayName = displayName;
        this.keyRequired = keyRequired;
        this.maxScoreLevels = maxScoreLevels;
        final var prefix = name + ".";
        apiKey = Field.create(prefix + "api.key")
                .withDisplayName(displayName + " API key.")
                .withType(ConfigDef.Type.PASSWORD)
                .withWidth(ConfigDef.Width.SHORT)
                .withImportance(ConfigDef.Importance.HIGH)
                .withDescription(keyRequired ? "API key required by the " + displayName + " API." : "Optional API key for the " + displayName + " API.");
        baseUrlField = Field.create(prefix + "base.url")
                .withDisplayName(displayName + " base URL.")
                .withType(ConfigDef.Type.STRING)
                .withWidth(ConfigDef.Width.SHORT)
                .withImportance(ConfigDef.Importance.LOW)
                .withDefault(baseUrl)
                .withDescription("Base URL of the " + displayName + " API, only the 'http' and 'https' schemes are allowed (defaults to " + baseUrl + ").");
        modelField = Field.create(prefix + "model")
                .withDisplayName(displayName + " model.")
                .withType(ConfigDef.Type.STRING)
                .withWidth(ConfigDef.Width.SHORT)
                .withImportance(ConfigDef.Importance.MEDIUM)
                .withDefault(model)
                .withDescription("Model or model alias used for the decisions (defaults to " + model + ").");
        longContext = longContextSupported ? Field.create(prefix + "long.context")
                .withDisplayName(displayName + " long context.")
                .withType(ConfigDef.Type.BOOLEAN)
                .withWidth(ConfigDef.Width.SHORT)
                .withImportance(ConfigDef.Importance.LOW)
                .withDefault(false)
                .withDescription("Whether to enable the long context (262k tokens instead of 64k tokens per request) of the " + displayName + " API.") : null;
        requestTimeoutMs = Field.create(prefix + "request.timeout.ms")
                .withDisplayName(displayName + " request timeout.")
                .withType(ConfigDef.Type.INT)
                .withWidth(ConfigDef.Width.SHORT)
                .withImportance(ConfigDef.Importance.MEDIUM)
                .withDefault(10_000)
                .withDescription("Milliseconds to wait for a single attempt of the " + displayName + " API call (defaults to 10000).")
                .withValidation(Field::isPositiveInteger);
        connectTimeoutMs = Field.create(prefix + "connect.timeout.ms")
                .withDisplayName(displayName + " connect timeout.")
                .withType(ConfigDef.Type.INT)
                .withWidth(ConfigDef.Width.SHORT)
                .withImportance(ConfigDef.Importance.LOW)
                .withDefault(5_000)
                .withDescription("Milliseconds to wait for the connection to the " + displayName + " API to be established (defaults to 5000).")
                .withValidation(Field::isPositiveInteger);
        retryMaxAttempts = Field.create(prefix + "retry.max.attempts")
                .withDisplayName(displayName + " maximum attempts.")
                .withType(ConfigDef.Type.INT)
                .withWidth(ConfigDef.Width.SHORT)
                .withImportance(ConfigDef.Importance.MEDIUM)
                .withDefault(3)
                .withDescription("Total number of attempts of a " + displayName + " API call including the first one (defaults to 3).")
                .withValidation(Field::isPositiveInteger);
        retryInitialDelayMs = Field.create(prefix + "retry.initial.delay.ms")
                .withDisplayName(displayName + " initial retry delay.")
                .withType(ConfigDef.Type.INT)
                .withWidth(ConfigDef.Width.SHORT)
                .withImportance(ConfigDef.Importance.LOW)
                .withDefault(500)
                .withDescription("Milliseconds to wait before the first retry, doubled for every further retry (defaults to 500).")
                .withValidation(Field::isNonNegativeInteger);
        retryMaxDelayMs = Field.create(prefix + "retry.max.delay.ms")
                .withDisplayName(displayName + " maximum retry delay.")
                .withType(ConfigDef.Type.INT)
                .withWidth(ConfigDef.Width.SHORT)
                .withImportance(ConfigDef.Importance.LOW)
                .withDefault(10_000)
                .withDescription("Upper bound in milliseconds of the delay between two attempts (defaults to 10000).")
                .withValidation(Field::isNonNegativeInteger);
        // Required fields are checked only for the selected provider.
        final var commonFields = Field.setOf(apiKey, baseUrlField, modelField, requestTimeoutMs, connectTimeoutMs, retryMaxAttempts,
                retryInitialDelayMs, retryMaxDelayMs);
        fields = longContextSupported ? commonFields.with(longContext) : commonFields;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Field.Set getConfigFields() {
        return fields;
    }

    @Override
    public void validateConfiguration(final Configuration config) {
        settings(config);
    }

    @Override
    public DecisionService create(final Configuration config, final List<Question> questions) {
        final var settings = settings(config);
        for (final var question : questions) {
            final var violation = SystemOneRequestMapper.limitViolation(question, displayName, maxScoreLevels);
            if (violation != null) {
                throw new ConfigException(violation);
            }
        }
        return new SystemOneDecisionService(settings);
    }

    SystemOneSettings settings(final Configuration config) {
        final var key = config.getString(apiKey);
        if (keyRequired && (key == null || key.isBlank())) {
            throw new ConfigException("'" + apiKey.name() + "' must be set to non-empty value.");
        }
        final var model = config.getString(modelField);
        if (model == null || model.isBlank()) {
            throw new ConfigException("'" + modelField.name() + "' must be set to non-empty value.");
        }
        return new SystemOneSettings(displayName, key == null || key.isBlank() ? null : key,
                SystemOneSettings.endpoint(config.getString(baseUrlField), baseUrlField.name()), model,
                longContext != null && config.getBoolean(longContext), maxScoreLevels,
                Duration.ofMillis(atLeast(config, requestTimeoutMs, 1)),
                Duration.ofMillis(atLeast(config, connectTimeoutMs, 1)),
                atLeast(config, retryMaxAttempts, 1),
                Duration.ofMillis(atLeast(config, retryInitialDelayMs, 0)),
                Duration.ofMillis(atLeast(config, retryMaxDelayMs, 0)));
    }

    private static int atLeast(final Configuration config, final Field field, final int min) {
        final int value;
        try {
            // Configuration.getInteger() falls back to the default for unparsable values.
            value = Integer.parseInt(config.getString(field).trim());
        }
        catch (NumberFormatException e) {
            throw new ConfigException("'" + field.name() + "' must be an integer.");
        }
        if (value < min) {
            throw new ConfigException("'" + field.name() + "' must be an integer of at least " + min + ".");
        }
        return value;
    }
}
