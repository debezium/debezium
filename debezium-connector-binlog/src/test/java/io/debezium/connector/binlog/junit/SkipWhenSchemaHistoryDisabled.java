/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Marker annotation for tests that inherently depend on the schema history (its storage, recovery, or the
 * schema change events emitted during a schema snapshot). Such tests are skipped when the testsuite runs
 * with the binlog-metadata-based schema mode enabled (see
 * {@link io.debezium.connector.binlog.util.UniqueDatabase#BINLOG_METADATA_BASED_SCHEMA_PROPERTY}), where no
 * schema history is used.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
@ExtendWith(SkipTestWhenSchemaHistoryDisabledExtension.class)
public @interface SkipWhenSchemaHistoryDisabled {

    /**
     * Returns the reason why the test depends on the schema history.
     */
    String reason() default "";
}
