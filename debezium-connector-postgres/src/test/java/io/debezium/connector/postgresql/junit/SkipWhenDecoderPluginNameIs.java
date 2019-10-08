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
 * tests to be skipped based on the decoder plugin name that is being used for testing.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface SkipWhenDecoderPluginNameIs {
    SkipWhenDecoderPluginNameIs.DecoderPluginName value();

    /**
     * Returns the reason why the test should be skipped.
     */
    String reason() default "";

    enum DecoderPluginName {
        WAL2JSON {
            @Override
            boolean isEqualTo(String pluginName) {
                return pluginName.equals("wal2json");
            }
        },
        WAL2JSON_STREAMING {
            @Override
            boolean isEqualTo(String pluginName) {
                return pluginName.equals("wal2json_streaming");
            }
        },
        WAL2JSON_RDS {
            @Override
            boolean isEqualTo(String pluginName) {
                return pluginName.equals("wal2json_rds");
            }
        },
        WAL2JSON_RDS_STREAMING {
            @Override
            boolean isEqualTo(String pluginName) {
                return pluginName.equals("wal2json_rds_streaming");
            }
        },
        DECODERBUFS {
            @Override
            boolean isEqualTo(String pluginName) {
                return pluginName.equals("decoderbufs");
            }
        },
        PGOUTPUT {
            @Override
            boolean isEqualTo(String pluginName) {
                return pluginName.equals("pgoutput");
            }
        };

        abstract boolean isEqualTo(String pluginName);
    }

}
