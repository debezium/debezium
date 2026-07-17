/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.time.LocalDate;
import java.time.LocalDateTime;

import io.debezium.time.StructuredTemporal;

/**
 * Inclusive range of calendar-based structured temporal values supported by a target data type.
 */
public record TemporalRange(Boundary minimum, Boundary maximum) {

    private static final TemporalRange UNBOUNDED = new TemporalRange(null, null);

    public TemporalRange {
        if (minimum != null && maximum != null && minimum.compareTo(maximum) > 0) {
            throw new IllegalArgumentException("Temporal range minimum must not be greater than its maximum");
        }
    }

    public static TemporalRange unbounded() {
        return UNBOUNDED;
    }

    public static TemporalRange dateYears(int minimumYear, int maximumYear) {
        return new TemporalRange(
                Boundary.date(minimumYear, 1, 1),
                Boundary.date(maximumYear, 12, 31));
    }

    public static TemporalRange timestampYears(int minimumYear, int maximumYear) {
        return new TemporalRange(
                Boundary.timestamp(minimumYear, 1, 1, 0, 0, 0, 0),
                Boundary.timestamp(maximumYear, 12, 31, 23, 59, 59, StructuredTemporal.PICOSECONDS_PER_SECOND - 1));
    }

    public boolean isBounded() {
        return minimum != null || maximum != null;
    }

    public boolean contains(Boundary value) {
        return (minimum == null || value.compareTo(minimum) >= 0)
                && (maximum == null || value.compareTo(maximum) <= 0);
    }

    public Boundary saturate(Boundary value) {
        if (minimum != null && value.compareTo(minimum) < 0) {
            return minimum;
        }
        if (maximum != null && value.compareTo(maximum) > 0) {
            return maximum;
        }
        return value;
    }

    public TemporalRange shiftSeconds(int seconds) {
        return new TemporalRange(
                minimum == null ? null : minimum.plusSeconds(seconds),
                maximum == null ? null : maximum.plusSeconds(seconds));
    }

    public TemporalRange intersect(TemporalRange other) {
        final Boundary intersectionMinimum;
        if (minimum == null) {
            intersectionMinimum = other.minimum;
        }
        else if (other.minimum == null) {
            intersectionMinimum = minimum;
        }
        else {
            intersectionMinimum = minimum.compareTo(other.minimum) >= 0 ? minimum : other.minimum;
        }

        final Boundary intersectionMaximum;
        if (maximum == null) {
            intersectionMaximum = other.maximum;
        }
        else if (other.maximum == null) {
            intersectionMaximum = maximum;
        }
        else {
            intersectionMaximum = maximum.compareTo(other.maximum) <= 0 ? maximum : other.maximum;
        }
        return new TemporalRange(intersectionMinimum, intersectionMaximum);
    }

    /**
     * Raw calendar components used to retain picosecond precision while comparing and saturating values.
     */
    public record Boundary(
            int year,
            int month,
            int day,
            int hour,
            int minute,
            int second,
            long picoseconds) implements Comparable<Boundary> {

        public Boundary {
            if (picoseconds < 0 || picoseconds >= StructuredTemporal.PICOSECONDS_PER_SECOND) {
                throw new IllegalArgumentException("Temporal boundary picoseconds must be between 0 and one second");
            }
        }

        public static Boundary date(int year, int month, int day) {
            return timestamp(year, month, day, 0, 0, 0, 0);
        }

        public static Boundary timestamp(int year, int month, int day, int hour, int minute, int second, long picoseconds) {
            return new Boundary(year, month, day, hour, minute, second, picoseconds);
        }

        public Boundary plusSeconds(int secondsToAdd) {
            if (secondsToAdd == 0) {
                return this;
            }
            final LocalDateTime adjusted = LocalDateTime.of(year, month, day, hour, minute, second).plusSeconds(secondsToAdd);
            return timestamp(adjusted.getYear(), adjusted.getMonthValue(), adjusted.getDayOfMonth(),
                    adjusted.getHour(), adjusted.getMinute(), adjusted.getSecond(), picoseconds);
        }

        public Boundary withPrecision(int precision) {
            if (precision >= 12) {
                return this;
            }
            if (precision < 0) {
                throw new IllegalArgumentException("Temporal precision must not be negative");
            }
            long factor = 1;
            for (int i = precision; i < 12; ++i) {
                factor *= 10;
            }
            return timestamp(year, month, day, hour, minute, second, picoseconds / factor * factor);
        }

        public LocalDate toLocalDate() {
            return LocalDate.of(year, month, day);
        }

        public LocalDateTime toLocalDateTime() {
            return LocalDateTime.of(year, month, day, hour, minute, second,
                    Math.toIntExact(picoseconds / StructuredTemporal.PICOSECONDS_PER_NANOSECOND));
        }

        @Override
        public int compareTo(Boundary other) {
            int result = Integer.compare(year, other.year);
            if (result == 0) {
                result = Integer.compare(month, other.month);
            }
            if (result == 0) {
                result = Integer.compare(day, other.day);
            }
            if (result == 0) {
                result = Integer.compare(hour, other.hour);
            }
            if (result == 0) {
                result = Integer.compare(minute, other.minute);
            }
            if (result == 0) {
                result = Integer.compare(second, other.second);
            }
            if (result == 0) {
                result = Long.compare(picoseconds, other.picoseconds);
            }
            return result;
        }

        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder(String.format("%04d-%02d-%02d %02d:%02d:%02d",
                    year, month, day, hour, minute, second));
            StructuredTemporalSupport.appendFraction(builder, picoseconds, 12);
            return builder.toString();
        }
    }
}
