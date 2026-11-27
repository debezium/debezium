/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision;

import java.util.List;

import io.debezium.ai.decision.api.DecisionService;
import io.debezium.ai.decision.api.Question;
import io.debezium.ai.decision.spi.DecisionServiceFactory;
import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Test provider registered under the name {@code stub}, see {@link StubDecisionService}.
 *
 * @author Jiri Pechanec
 */
public class StubDecisionServiceFactory implements DecisionServiceFactory {

    @Override
    public String name() {
        return "stub";
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf();
    }

    @Override
    public void validateConfiguration(final Configuration config) {
    }

    @Override
    public DecisionService create(final Configuration config, final List<Question> questions) {
        return new StubDecisionService();
    }
}
