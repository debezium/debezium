package io.debezium.jdbc;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.relational.Column;
import io.debezium.time.*;

public class ColumnBuilderHelper {

    public static SchemaBuilder dateBuilder(Column column, JdbcValueConverters.ValueConverterConfiguration configuration) {
        if (configuration.adaptiveTimePrecisionMode || configuration.adaptiveTimeMicrosecondsPrecisionMode) {
            return Date.builder();
        }
        return org.apache.kafka.connect.data.Date.builder();
    }

    public static SchemaBuilder timeBuilder(Column column, JdbcValueConverters.ValueConverterConfiguration configuration) {
        if (configuration.adaptiveTimeMicrosecondsPrecisionMode) {
            return MicroTime.builder();
        }
        if (configuration.adaptiveTimePrecisionMode) {
            if (configuration.timeprecision.apply(column) <= 3) {
                return Time.builder();
            }
            if (configuration.timeprecision.apply(column) <= 6) {
                return MicroTime.builder();
            }
            return NanoTime.builder();
        }
        return org.apache.kafka.connect.data.Time.builder();
    }

    public static SchemaBuilder timestampBuilder(Column column, JdbcValueConverters.ValueConverterConfiguration configuration) {
        if (configuration.adaptiveTimePrecisionMode || configuration.adaptiveTimeMicrosecondsPrecisionMode) {
            if (configuration.timeprecision.apply(column) <= 3) {
                return Timestamp.builder();
            }
            if (configuration.timeprecision.apply(column) <= 6) {
                return MicroTimestamp.builder();
            }
            return NanoTimestamp.builder();
        }
        return org.apache.kafka.connect.data.Timestamp.builder();
    }

}
