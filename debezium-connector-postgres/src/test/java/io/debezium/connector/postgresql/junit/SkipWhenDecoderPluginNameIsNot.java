/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation used together with the {@link SkipTestDependingOnDecoderPluginNameRule} JUnit rule, that allows
 * tests to be skipped based on the decoder plugin name that is not being used for testing.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface SkipWhenDecoderPluginNameIsNot {
    SkipWhenDecoderPluginNameIsNot.DecoderPluginName value();

    /**
     * Returns the reason why the test should be skipped.
     */
    String reason();

    enum DecoderPluginName {
        DECODERBUFS {
            @Override
            boolean isNotEqualTo(String pluginName) {
                return !pluginName.equals("decoderbufs");
            }
        },
        PGOUTPUT {
            @Override
            boolean isNotEqualTo(String pluginName) {;
                return !pluginName.equals("pgoutput");
            }
        };

        abstract boolean isNotEqualTo(String pluginName);
    }

}
