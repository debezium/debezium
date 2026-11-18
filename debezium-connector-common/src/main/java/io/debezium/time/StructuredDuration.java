/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Structured duration semantic type that preserves calendar and clock duration components.
 */
public final class StructuredDuration {

    public static final String SCHEMA_NAME = "io.debezium.time.StructuredDuration";

    /**
     * The kind of a duration, which determines which components a value may have and whether they can be converted into
     * each other without changing its meaning.
     */
    public enum Kind {
        /**
         * Years and months, which convert into a single signed number of months, such as an Oracle
         * {@code INTERVAL YEAR TO MONTH}.
         */
        YEAR_MONTH("year-month", "YearMonth"),
        /**
         * Days, hours, minutes, and seconds, which convert into a single signed number of seconds because a day has
         * exactly 24 hours, such as a MySQL {@code TIME} or an Oracle {@code INTERVAL DAY TO SECOND}.
         */
        DAY_TIME("day-time", "DayTime"),
        /**
         * Months, days, and time as independent counters with their own signs, such as a PostgreSQL {@code INTERVAL}.
         * A month is not fixed at 30 days and a day is not fixed at 24 hours, so the counters cannot be combined.
         */
        MONTH_DAY_TIME("month-day-time", "MonthDayTime");

        private final String value;
        private final String schemaNameComponent;

        Kind(String value, String schemaNameComponent) {
            this.value = value;
            this.schemaNameComponent = schemaNameComponent;
        }

        /**
         * Returns the name of the kind that messages and documentation use.
         *
         * @return the name, such as {@code day-time}
         */
        public String getValue() {
            return value;
        }

        /**
         * Returns the component that identifies the kind in a logical schema name. It is part of the schema names that
         * consumers and schema registries see, so it must not change with the name of the enum constant.
         *
         * @return the schema name component, such as {@code DayTime}
         */
        public String getSchemaNameComponent() {
            return schemaNameComponent;
        }
    }

    public static SchemaBuilder builder() {
        return builder(-1, null);
    }

    public static SchemaBuilder builder(int precision, Kind kind) {
        return StructuredTemporal.withPrecision(SchemaBuilder.struct()
                .name(schemaName(precision, kind))
                .version(1)
                .field(StructuredTemporal.YEARS_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.MONTHS_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.DAYS_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.HOURS_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.MINUTES_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.SECONDS_FIELD, StructuredTemporal.optionalInt64())
                .field(StructuredTemporal.PICOSECONDS_FIELD, StructuredTemporal.optionalInt64())
                .field(StructuredTemporal.PRECISION_FIELD, StructuredTemporal.optionalInt32()), precision);
    }

    /**
     * Returns the logical schema name of a structured duration. The kind is part of the semantic type name and precedes
     * the precision suffix that all structured temporal types share, for example,
     * {@code io.debezium.time.StructuredDurationDayTimePrecision9}.
     *
     * @param precision the fractional-second precision, or a negative value if it is not known
     * @param kind the duration kind; may be null if it is not known
     * @return the schema name
     */
    public static String schemaName(int precision, Kind kind) {
        final String typeName = kind == null ? SCHEMA_NAME : SCHEMA_NAME + kind.getSchemaNameComponent();
        return StructuredTemporal.schemaName(typeName, precision);
    }

    /**
     * Returns the duration kind that a logical schema name identifies. A source-column qualifier is ignored.
     *
     * @param schemaName the schema name; may be null
     * @return the kind, or empty if the name does not identify one, for example, because it does not include a kind
     */
    public static Optional<Kind> kindOf(String schemaName) {
        final String typeName = StructuredTemporal.schemaNameWithoutSourceColumn(schemaName);
        if (typeName == null || !typeName.startsWith(SCHEMA_NAME)) {
            return Optional.empty();
        }
        final String suffix = typeName.substring(SCHEMA_NAME.length());
        for (Kind kind : Kind.values()) {
            final String component = kind.getSchemaNameComponent();
            if (suffix.startsWith(component) && isPrecisionSuffix(suffix.substring(component.length()))) {
                return Optional.of(kind);
            }
        }
        return Optional.empty();
    }

    private static boolean isPrecisionSuffix(String suffix) {
        return suffix.isEmpty() || suffix.matches("Precision\\d+");
    }

    public static String[] schemaNames() {
        final List<String> names = new ArrayList<>();
        names.add(SCHEMA_NAME);
        addSchemaNames(names, -1);
        for (int precision = 0; precision <= StructuredTemporal.MAX_FRACTIONAL_SECOND_PRECISION; ++precision) {
            addSchemaNames(names, precision);
        }
        return names.toArray(String[]::new);
    }

    private static void addSchemaNames(List<String> names, int precision) {
        if (precision >= 0) {
            names.add(schemaName(precision, null));
        }
        for (Kind kind : Kind.values()) {
            names.add(schemaName(precision, kind));
        }
    }

    public static Schema schema() {
        return builder().build();
    }

    public static Struct from(Schema schema, int years, int months, int days, int hours, int minutes, long seconds, int nanos) {
        return from(schema, years, months, days, hours, minutes, seconds, nanos, -1);
    }

    public static Struct from(Schema schema, int years, int months, int days, int hours, int minutes, long seconds, int nanos, int precision) {
        return fromPicoseconds(schema, years, months, days, hours, minutes, seconds, StructuredTemporal.picosecondsFromNanoseconds(nanos), precision);
    }

    public static Struct fromPicoseconds(Schema schema, int years, int months, int days, int hours, int minutes, long seconds, long picoseconds,
                                         int precision) {
        return StructuredTemporal.withPrecision(new Struct(schema)
                .put(StructuredTemporal.YEARS_FIELD, years)
                .put(StructuredTemporal.MONTHS_FIELD, months)
                .put(StructuredTemporal.DAYS_FIELD, days)
                .put(StructuredTemporal.HOURS_FIELD, hours)
                .put(StructuredTemporal.MINUTES_FIELD, minutes)
                .put(StructuredTemporal.SECONDS_FIELD, seconds)
                .put(StructuredTemporal.PICOSECONDS_FIELD, picoseconds), precision);
    }

    private StructuredDuration() {
    }
}
