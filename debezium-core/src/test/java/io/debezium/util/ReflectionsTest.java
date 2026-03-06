/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class ReflectionsTest {

    @Test
    public void shouldInvokeMethodWithNewNameWhenPresent() {
        String result = Reflections.invokeMethodWithFallbackName(new TestMethods(), "newName", "oldName", String.class);
        assertEquals("new", result);
    }

    @Test
    public void shouldFallbackToOldMethodNameWhenNewNameDoesNotExist() {
        String result = Reflections.invokeMethodWithFallbackName(new TestMethods(), "newNameMissing", "oldName", String.class);
        assertEquals("old", result);
    }

    @Test
    public void shouldHandleMethodsWithoutParameters() {
        String result = Reflections.invokeMethodWithFallbackName(new TestMethods(), "newNoArg", "oldNoArg", String.class);
        assertEquals("new-no-arg", result);
    }

    @Test
    public void shouldThrowWhenNeitherMethodExists() {
        assertThrows(IllegalStateException.class, () -> Reflections.invokeMethodWithFallbackName(new TestMethods(), "missingOne", "missingTwo", String.class));
    }

    public static class TestMethods {

        public String newName() {
            return "new";
        }

        public String oldName() {
            return "old";
        }

        public String newNoArg() {
            return "new-no-arg";
        }

        public String oldNoArg() {
            return "old-no-arg";
        }
    }
}
