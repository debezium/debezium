/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation used together with the {@link SkipTestDependingOnAdapterNameRule} JUnit rule, that allows
 * tests to be skipped based on the adapter name that is being used for testing.
 *
 * @author Chris Cranford
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface SkipWhenAdapterNameIs {

    SkipWhenAdapterNameIs.AdapterName value();

    /**
     * Returns the reason why the test should be skipped.
     */
    String reason() default "";

    enum AdapterName {
        XSTREAM {
            @Override
            boolean isEqualTo(String adapterName) {
                return adapterName.equalsIgnoreCase("xstream");
            }
        },
        LOGMINER {
            @Override
            boolean isEqualTo(String adapterName) {
                return adapterName.equalsIgnoreCase("logminer") || adapterName.equalsIgnoreCase("logminer_unbuffered");
            }
        },
        LOGMINER_BUFFERED {
            @Override
            boolean isEqualTo(String adapterName) {
                return adapterName.equalsIgnoreCase("logminer");
            }
        },
        LOGMINER_UNBUFFERED {
            @Override
            boolean isEqualTo(String adapterName) {
                return adapterName.equalsIgnoreCase("logminer_unbuffered");
            }
        },
        OLR {
            @java.lang.Override
            boolean isEqualTo(String adapterName) {
                return adapterName.equalsIgnoreCase("olr");
            }
        };

        abstract boolean isEqualTo(String adapterName);
    }
}
