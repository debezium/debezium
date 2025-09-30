/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app.conditions;

import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class SingleEngineCondition implements ExecutionCondition {
    private static final ConditionEvaluationResult ENABLED = ConditionEvaluationResult.enabled("SingleEngine enabled with profile --prod");
    private static final ConditionEvaluationResult DISABLED = ConditionEvaluationResult.disabled("SingleEngine disable with profile --multi");

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        if (ConfigProvider.getConfig()
                .getConfigValue("quarkus.test.integration-test-profile")
                .getValue()
                .equals("prod")) {
            return DISABLED;
        }

        return ENABLED;
    }
}
