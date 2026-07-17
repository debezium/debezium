/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.util.EnumSet;
import java.util.Set;

import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.time.StructuredDuration;

/**
 * Describes the structured temporal values that a target dialect can preserve.
 */
public record TargetTemporalCapabilities(
        int maxTimePrecision,
        int maxTimestampPrecision,
        ZonedTimestampSupport zonedTimestampSupport,
        Set<StructuredDuration.Kind> durationKinds,
        boolean timeColumnPrecisionReliable,
        boolean timestampColumnPrecisionReliable) {

    public TargetTemporalCapabilities {
        durationKinds = Set.copyOf(durationKinds);
    }

    public static TargetTemporalCapabilities defaults(int maxTimePrecision, int maxTimestampPrecision) {
        return new TargetTemporalCapabilities(
                maxTimePrecision,
                maxTimestampPrecision,
                ZonedTimestampSupport.NONE,
                EnumSet.allOf(StructuredDuration.Kind.class),
                true,
                true);
    }

    public TargetTemporalCapabilities withZonedTimestampSupport(ZonedTimestampSupport support) {
        return new TargetTemporalCapabilities(maxTimePrecision, maxTimestampPrecision, support, durationKinds,
                timeColumnPrecisionReliable, timestampColumnPrecisionReliable);
    }

    public TargetTemporalCapabilities withDurationKinds(Set<StructuredDuration.Kind> kinds) {
        return new TargetTemporalCapabilities(maxTimePrecision, maxTimestampPrecision, zonedTimestampSupport, kinds,
                timeColumnPrecisionReliable, timestampColumnPrecisionReliable);
    }

    public TargetTemporalCapabilities withTimestampColumnPrecisionReliable(boolean reliable) {
        return new TargetTemporalCapabilities(maxTimePrecision, maxTimestampPrecision, zonedTimestampSupport, durationKinds,
                timeColumnPrecisionReliable, reliable);
    }

    public int targetTimePrecision(ColumnDescriptor column) {
        return targetPrecision(column, maxTimePrecision, timeColumnPrecisionReliable);
    }

    public int targetTimestampPrecision(ColumnDescriptor column) {
        return targetPrecision(column, maxTimestampPrecision, timestampColumnPrecisionReliable);
    }

    private static int targetPrecision(ColumnDescriptor column, int maxPrecision, boolean columnPrecisionReliable) {
        if (!columnPrecisionReliable) {
            return maxPrecision;
        }
        final int columnPrecision = column.getScale();
        return columnPrecision < 0 ? maxPrecision : Math.min(columnPrecision, maxPrecision);
    }

    public enum ZonedTimestampSupport {
        NONE,
        INSTANT,
        OFFSET,
        REGION;

        public boolean preservesOffset() {
            return this == OFFSET || this == REGION;
        }

        public boolean preservesRegion() {
            return this == REGION;
        }
    }
}
