/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime.events;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.inject.Qualifier;

@Qualifier
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
public @interface CaptureGroup {
    String value();

    final class Literal extends AnnotationLiteral<CaptureGroup> implements CaptureGroup {
        private final String value;

        private Literal(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }

        public static Literal of(String value) {
            return new Literal(value);
        }
    }
}
