/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app.test;

import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class MultiEngineCondition implements ExecutionCondition {
    private static final ConditionEvaluationResult ENABLED = ConditionEvaluationResult.enabled("MultiEngine enabled with profile --multi");
    private static final ConditionEvaluationResult DISABLED = ConditionEvaluationResult.disabled("MultiEngine disable with profile --prod");

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        if (ConfigProvider.getConfig()
                .getConfigValue("quarkus.test.integration-test-profile")
                .getValue()
                .equals("multi")) {
            return DISABLED;
        }

        return ENABLED;
    }
}
