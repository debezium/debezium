/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation used together with the {@link SkipTestRule} JUnit rule, that allows tests to be skipped
 * based on the Apache Kafka version used for testing.
 *
 * @author Chris Cranford
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface SkipWhenKafkaVersion {

    KafkaVersion value();

    EqualityCheck check();

    String description() default "";

    public enum KafkaVersion {
        KAFKA_1XX {
            @Override
            boolean isLessThan(int major, int minor, int patch) {
                return major < 1;
            }

            @Override
            boolean isLessThanOrEqualTo(int major, int minor, int patch) {
                return isLessThan(major, minor, patch) || isEqualTo(major, minor, patch);
            }

            @Override
            boolean isEqualTo(int major, int minor, int patch) {
                return major == 1;
            }

            @Override
            boolean isGreaterThanOrEqualTo(int major, int minor, int patch) {
                return major > 1 || isEqualTo(major, minor, patch);
            }

            @Override
            boolean isGreaterThan(int major, int minor, int patch) {
                return major > 1;
            }
        },

        KAFKA_241 {
            @Override
            boolean isLessThan(int major, int minor, int patch) {
                return major < 2 || (major == 2 && minor < 4) || (major == 2 && minor == 4 && patch < 1);
            }

            @Override
            boolean isLessThanOrEqualTo(int major, int minor, int patch) {
                return isLessThan(major, minor, patch) || isEqualTo(major, minor, patch);
            }

            @Override
            boolean isEqualTo(int major, int minor, int patch) {
                return major == 2 && minor == 4 && patch == 1;
            }

            @Override
            boolean isGreaterThanOrEqualTo(int major, int minor, int patch) {
                return !isLessThan(major, minor, patch) || isEqualTo(major, minor, patch);
            }

            @Override
            boolean isGreaterThan(int major, int minor, int patch) {
                return !isLessThan(major, minor, patch) && !isEqualTo(major, minor, patch);
            }
        };

        abstract boolean isLessThan(int major, int minor, int patch);

        abstract boolean isLessThanOrEqualTo(int major, int minor, int patch);

        abstract boolean isEqualTo(int major, int minor, int patch);

        abstract boolean isGreaterThanOrEqualTo(int major, int minor, int patch);

        abstract boolean isGreaterThan(int major, int minor, int patch);
    }
}
