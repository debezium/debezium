/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics.traits;

/**
 * // TODO: Document this
 *
 * @author vjuranek
 * @since 4.0
 */
public interface StreamingStatisticsMXBean {
    Long getMilliSecondsBehindSourceMinValue();

    Long getMilliSecondsBehindSourceMaxValue();

    Long getMilliSecondsBehindSourceAverageValue();

    Double getMilliSecondsBehindSourceP50();

    Double getMilliSecondsBehindSourceP95();

    Double getMilliSecondsBehindSourceP99();
}
