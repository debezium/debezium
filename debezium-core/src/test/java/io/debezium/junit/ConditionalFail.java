/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * JUnit rule that inspects the presence of the {@link ShouldFailWhen} annotation on a test method.
 * If it finds the annotation, it will modify pass/fail report of test depending on the condition
 * defined in the annotation.
 *
 * @author Jiri Pechanec
 */
public class ConditionalFail extends AnnotationBasedTestRule {

    @Override
    public Statement apply(final Statement base, final Description description) {
        final ShouldFailWhen conditionClass = hasAnnotation(description, ShouldFailWhen.class);
        if (conditionClass == null) {
            return base;
        }
        try {
            Supplier<Boolean> condition = conditionClass.value().getDeclaredConstructor().newInstance();
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    Throwable failure = null;
                    try {
                        base.evaluate();
                    }
                    catch (final Throwable t) {
                        failure = t;
                    }
                    if (condition.get() && failure == null) {
                        Assert.fail("Expected failing test for " + description);
                    }
                    else if (condition.get() && failure != null) {
                        System.out.println("Ignored failure for " + description);
                    }
                    else if (failure != null) {
                        throw failure;
                    }
                }
            };
        }
        catch (final InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }
}
