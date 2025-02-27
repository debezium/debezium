/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * JUnit rule that inspects the presence of the {@link RequiresAssemblyProfile} annotation either on a test method or on a test suite. If
 * it finds the annotation, it will only run the test method/suite if the system property {@code isAssemblyProfileActive} has the
 * value {@code true}
 *
 * @author René Kerner
 */
public class RequiresAssemblyProfileTestRule extends AnnotationBasedTestRule {

    @Override
    public Statement apply(Statement base, Description description) {
        RequiresAssemblyProfile requiresAssemblyProfileAnnotation = hasAnnotation(description, RequiresAssemblyProfile.class);
        if (requiresAssemblyProfileAnnotation != null) {
            String requiresAssemblyProfile = System.getProperty(RequiresAssemblyProfile.REQUIRES_ASSEMBLY_PROFILE_PROPERTY, "false");
            if (!Boolean.parseBoolean(requiresAssemblyProfile)) {
                return emptyStatement(requiresAssemblyProfileAnnotation.value(), description);
            }
        }
        return base;
    }

}
