/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.runtime.events;

import static io.debezium.runtime.EngineManifest.DEFAULT;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import jakarta.enterprise.event.Event;
import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.inject.Qualifier;

import io.debezium.runtime.EngineManifest;

@Qualifier
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
public @interface DefaultEngine {
    class Literal extends AnnotationLiteral<DefaultEngine> implements DefaultEngine {
        public static <T> Event<T> selectDefault(Event<T> event, EngineManifest manifest) {
            if (DEFAULT.equals(manifest)) {
                return event.select(new DefaultEngine.Literal());
            }
            return event;
        }
    }
}
