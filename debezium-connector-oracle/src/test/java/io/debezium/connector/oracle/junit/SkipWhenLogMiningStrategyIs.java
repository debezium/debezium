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

import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningStrategy;

/**
 * Marker annotation used together with the {@link SkipTestDependingOnStrategyRule} JUnit rule, that allows
 * tests to be skipped when on whether the log mining strategy is set to the given value. This annotation
 * also implicitly requires to skip that the adapter is also LogMiner.
 *
 * @author Chris Cranford
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface SkipWhenLogMiningStrategyIs {

    SkipWhenLogMiningStrategyIs.Strategy value();

    /**
     * Returns the reason why the test is skipped.
     */
    String reason() default "";

    enum Strategy {
        CATALOG_IN_REDO {
            @Override
            boolean isEqualTo(String miningStrategy) {
                return LogMiningStrategy.CATALOG_IN_REDO.getValue().equalsIgnoreCase(miningStrategy);
            }
        },
        ONLINE_CATALOG {
            @Override
            boolean isEqualTo(String miningStrategy) {
                return LogMiningStrategy.ONLINE_CATALOG.getValue().equalsIgnoreCase(miningStrategy);
            }
        },
        HYBRID {
            @Override
            boolean isEqualTo(String miningStrategy) {
                return LogMiningStrategy.HYBRID.getValue().equalsIgnoreCase(miningStrategy);
            }
        };

        abstract boolean isEqualTo(String miningStrategy);
    }
}
