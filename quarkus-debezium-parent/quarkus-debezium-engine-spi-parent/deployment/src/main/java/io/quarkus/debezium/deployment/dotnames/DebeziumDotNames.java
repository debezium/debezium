/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.dotnames;

import org.jboss.jandex.DotName;
import org.jboss.jandex.MethodInfo;

import io.debezium.runtime.Capturing;
import io.quarkus.arc.processor.BeanInfo;

public class DebeziumDotNames {
    public static class CapturingDotName {
        public static final DotName CAPTURING = DotName.createSimple(Capturing.class.getName());

        public static boolean filter(MethodInfo info) {
            return info.annotations()
                    .stream()
                    .anyMatch(instance -> CAPTURING.equals(instance.name()));
        }

        public static boolean filter(BeanInfo info) {
            return info.getTarget()
                    .map(annotation -> annotation.asClass().methods()
                            .stream()
                            .anyMatch(CapturingDotName::filter))
                    .orElse(false);
        }
    }
}
