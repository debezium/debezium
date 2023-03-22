/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter.e2e.source;

import io.debezium.jdbc.TemporalPrecisionMode;

/**
 * A parameter object for passing source connector options that may vary by test matrix invocation.
 *
 * @author Chris Cranford
 */
public interface SourceConnectorOptions {
    /**
     * Whether to test capturing changes via snapshot when true, streaming when false.
     */
    boolean useSnapshot();

    /**
     * Whether to execute tests applying default values.
     */
    boolean useDefaultValues();

    /**
     * Returns whether the source connector should be deployed using {@code ExtractNewRecordState}.
     */
    boolean isFlatten();

    /**
     * Returns whether column type propagation is enabled; defaults to {@code false}.
     */
    boolean isColumnTypePropagated();

    /**
     * Returns the temporal precision mode.
     */
    TemporalPrecisionMode getTemporalPrecisionMode();
}
