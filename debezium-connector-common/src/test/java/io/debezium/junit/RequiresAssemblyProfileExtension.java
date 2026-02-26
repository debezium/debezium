/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * JUnit 5 Extension that inspects the presence of the {@link RequiresAssemblyProfile} annotation either on a test method or on a test class.
 * If it finds the annotation, it will only run the test method/class if the system property {@code isAssemblyProfileActive} has the
 * value {@code true}.
 *
 * @author René Kerner
 */
public class RequiresAssemblyProfileExtension extends AnnotationBasedExtension implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        RequiresAssemblyProfile requiresAssemblyProfileAnnotation = hasAnnotation(context, RequiresAssemblyProfile.class);
        if (requiresAssemblyProfileAnnotation != null) {
            String requiresAssemblyProfile = System.getProperty(RequiresAssemblyProfile.REQUIRES_ASSEMBLY_PROFILE_PROPERTY, "false");
            if (!Boolean.parseBoolean(requiresAssemblyProfile)) {
                String message = formatSkipMessage(requiresAssemblyProfileAnnotation.value(), context);
                System.out.println(message);
                return ConditionEvaluationResult.disabled(message);
            }
        }

        return ConditionEvaluationResult.enabled("Assembly profile check passed");
    }
}
