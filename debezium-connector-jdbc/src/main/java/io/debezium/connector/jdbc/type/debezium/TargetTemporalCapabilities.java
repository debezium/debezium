/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
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
        boolean timestampColumnPrecisionReliable,
        TemporalRange dateRange,
        TemporalRange timestampRange,
        Map<String, TemporalRange> timestampRangesByType,
        ZonedTimestampRangeBasis zonedTimestampRangeBasis) {

    public TargetTemporalCapabilities {
        durationKinds = Set.copyOf(durationKinds);
        timestampRangesByType = Map.copyOf(timestampRangesByType);
    }

    public static TargetTemporalCapabilities defaults(int maxTimePrecision, int maxTimestampPrecision) {
        return new TargetTemporalCapabilities(
                maxTimePrecision,
                maxTimestampPrecision,
                ZonedTimestampSupport.NONE,
                EnumSet.allOf(StructuredDuration.Kind.class),
                true,
                true,
                TemporalRange.unbounded(),
                TemporalRange.unbounded(),
                Map.of(),
                ZonedTimestampRangeBasis.LOCAL);
    }

    public TargetTemporalCapabilities withZonedTimestampSupport(ZonedTimestampSupport support) {
        return new TargetTemporalCapabilities(maxTimePrecision, maxTimestampPrecision, support, durationKinds,
                timeColumnPrecisionReliable, timestampColumnPrecisionReliable, dateRange, timestampRange,
                timestampRangesByType, zonedTimestampRangeBasis);
    }

    public TargetTemporalCapabilities withDurationKinds(Set<StructuredDuration.Kind> kinds) {
        return new TargetTemporalCapabilities(maxTimePrecision, maxTimestampPrecision, zonedTimestampSupport, kinds,
                timeColumnPrecisionReliable, timestampColumnPrecisionReliable, dateRange, timestampRange,
                timestampRangesByType, zonedTimestampRangeBasis);
    }

    public TargetTemporalCapabilities withTimestampColumnPrecisionReliable(boolean reliable) {
        return new TargetTemporalCapabilities(maxTimePrecision, maxTimestampPrecision, zonedTimestampSupport, durationKinds,
                timeColumnPrecisionReliable, reliable, dateRange, timestampRange, timestampRangesByType,
                zonedTimestampRangeBasis);
    }

    public TargetTemporalCapabilities withDateRange(TemporalRange range) {
        return new TargetTemporalCapabilities(maxTimePrecision, maxTimestampPrecision, zonedTimestampSupport, durationKinds,
                timeColumnPrecisionReliable, timestampColumnPrecisionReliable, range, timestampRange,
                timestampRangesByType, zonedTimestampRangeBasis);
    }

    public TargetTemporalCapabilities withTimestampRange(TemporalRange range) {
        return new TargetTemporalCapabilities(maxTimePrecision, maxTimestampPrecision, zonedTimestampSupport, durationKinds,
                timeColumnPrecisionReliable, timestampColumnPrecisionReliable, dateRange, range,
                timestampRangesByType, zonedTimestampRangeBasis);
    }

    public TargetTemporalCapabilities withTimestampRangeForType(TemporalRange range, String... typeNames) {
        final Map<String, TemporalRange> ranges = new HashMap<>(timestampRangesByType);
        for (String typeName : typeNames) {
            ranges.put(normalizeTypeName(typeName), range);
        }
        return new TargetTemporalCapabilities(maxTimePrecision, maxTimestampPrecision, zonedTimestampSupport, durationKinds,
                timeColumnPrecisionReliable, timestampColumnPrecisionReliable, dateRange, timestampRange, ranges,
                zonedTimestampRangeBasis);
    }

    public TargetTemporalCapabilities withZonedTimestampRangeBasis(ZonedTimestampRangeBasis basis) {
        return new TargetTemporalCapabilities(maxTimePrecision, maxTimestampPrecision, zonedTimestampSupport, durationKinds,
                timeColumnPrecisionReliable, timestampColumnPrecisionReliable, dateRange, timestampRange,
                timestampRangesByType, basis);
    }

    public int targetTimePrecision(ColumnDescriptor column) {
        return targetPrecision(column, maxTimePrecision, timeColumnPrecisionReliable);
    }

    public int targetTimestampPrecision(ColumnDescriptor column) {
        return targetPrecision(column, maxTimestampPrecision, timestampColumnPrecisionReliable);
    }

    public TemporalRange targetDateRange(ColumnDescriptor column) {
        return dateRange;
    }

    public TemporalRange targetTimestampRange(ColumnDescriptor column) {
        if (column != null && column.getTypeName() != null) {
            final TemporalRange columnTypeRange = timestampRangesByType.get(normalizeTypeName(column.getTypeName()));
            if (columnTypeRange != null) {
                return columnTypeRange;
            }
        }
        return timestampRange;
    }

    public TemporalRange targetZonedTimestampRange(ColumnDescriptor column, int offsetSeconds) {
        final TemporalRange range = targetTimestampRange(column);
        return switch (zonedTimestampRangeBasis) {
            case LOCAL -> range;
            case INSTANT -> range.shiftSeconds(offsetSeconds);
            case LOCAL_AND_INSTANT -> range.intersect(range.shiftSeconds(offsetSeconds));
        };
    }

    private static int targetPrecision(ColumnDescriptor column, int maxPrecision, boolean columnPrecisionReliable) {
        if (!columnPrecisionReliable) {
            return maxPrecision;
        }
        final int columnPrecision = column.getScale();
        return columnPrecision < 0 ? maxPrecision : Math.min(columnPrecision, maxPrecision);
    }

    private static String normalizeTypeName(String typeName) {
        return typeName.toLowerCase(Locale.ROOT)
                .replaceAll("\\([^)]*\\)", "")
                .trim()
                .replaceAll("\\s+", " ");
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

    public enum ZonedTimestampRangeBasis {
        LOCAL,
        INSTANT,
        LOCAL_AND_INSTANT
    }
}
