/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.spi;

import java.util.List;

import io.debezium.ai.decision.api.DecisionService;
import io.debezium.ai.decision.api.Question;
import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Factory of {@link DecisionService} implementations, discovered via {@link java.util.ServiceLoader}.
 * The provider is selected by the {@code decision.provider} option of the SMT.
 * <p>
 * Provider specific {@link Field}s must use the {@code <name>.} prefix and must not be declared as required,
 * as the fields of all discovered providers are validated together.
 * The mandatory options of the selected provider are enforced in {@link #validateConfiguration(Configuration)}.
 *
 * @author Jiri Pechanec
 */
public interface DecisionServiceFactory {

    /**
     * @return provider id, lower case, e.g. {@code jev}
     */
    String name();

    /**
     * @return provider specific options
     */
    Field.Set getConfigFields();

    /**
     * Validates the provider specific options.
     *
     * @throws org.apache.kafka.common.config.ConfigException if the configuration is not valid
     */
    void validateConfiguration(Configuration config);

    /**
     * Creates the service. Provider specific limits of the questions are checked here so a violation fails at startup.
     *
     * @throws org.apache.kafka.common.config.ConfigException if the configuration or the questions are not valid
     */
    DecisionService create(Configuration config, List<Question> questions);
}
