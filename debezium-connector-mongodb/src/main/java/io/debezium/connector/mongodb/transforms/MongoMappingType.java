/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.BitSet;
import java.util.Locale;
import java.util.UUID;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import io.debezium.data.Bits;
import io.debezium.data.Json;
import io.debezium.data.Uuid;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.time.MicroDuration;
import io.debezium.time.MicroTime;
import io.debezium.time.NanoDuration;
import io.debezium.time.NanoTime;
import io.debezium.time.Year;
import io.debezium.time.ZonedTime;

/**
 * Defines mapping schemas and converts selected BSON values to their Connect representations.
 *
 * @author Divyansh Agrawal
 */
final class MongoMappingType {
    private static final JsonWriterSettings JSON_SETTINGS = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

    private MongoMappingType() {
    }

    static Schema schema(String fieldType, Integer scale, Integer precision, Integer length) {
        if (scale != null || precision != null) {
            if (!Decimal.LOGICAL_NAME.equals(fieldType)) {
                throw new ConfigException("scale and precision apply only to " + Decimal.LOGICAL_NAME);
            }
        }
        if (length != null && !Bits.LOGICAL_NAME.equals(fieldType)) {
            throw new ConfigException("length applies only to " + Bits.LOGICAL_NAME);
        }
        // Logical names are case-sensitive, as in Kafka Connect.
        switch (fieldType) {
            case "io.debezium.time.Date":
                return io.debezium.time.Date.builder().optional().build();
            case "io.debezium.time.Timestamp":
                return io.debezium.time.Timestamp.builder().optional().build();
            case "io.debezium.time.MicroTimestamp":
                return io.debezium.time.MicroTimestamp.builder().optional().build();
            case "io.debezium.time.NanoTimestamp":
                return io.debezium.time.NanoTimestamp.builder().optional().build();
            case "io.debezium.time.ZonedTimestamp":
                return io.debezium.time.ZonedTimestamp.builder().optional().build();
            case "io.debezium.time.Time":
                return io.debezium.time.Time.builder().optional().build();
            case "io.debezium.time.MicroTime":
                return MicroTime.builder().optional().build();
            case "io.debezium.time.NanoTime":
                return NanoTime.builder().optional().build();
            case "io.debezium.time.ZonedTime":
                return ZonedTime.builder().optional().build();
            case "io.debezium.time.Year":
                return Year.builder().optional().build();
            case "io.debezium.time.MicroDuration":
                return MicroDuration.builder().optional().build();
            case "io.debezium.time.NanoDuration":
                return NanoDuration.builder().optional().build();
            case "io.debezium.data.Bits":
                if (length == null || length <= 0) {
                    throw new ConfigException("Bits requires a positive length");
                }
                return Bits.builder(length).optional().build();
            case "io.debezium.data.Json":
                return Json.builder().optional().build();
            case "io.debezium.data.Uuid":
                return Uuid.builder().optional().build();
            case "io.debezium.data.VariableScaleDecimal":
                return VariableScaleDecimal.builder().optional().build();
            case "org.apache.kafka.connect.data.Decimal":
                return decimalSchema(scale, precision);
            case "org.apache.kafka.connect.data.Timestamp":
                return Timestamp.builder().optional().build();
            case "org.apache.kafka.connect.data.Date":
                return Date.builder().optional().build();
            case "org.apache.kafka.connect.data.Time":
                return Time.builder().optional().build();
            default:
                break;
        }

        // Fall back to primitive type resolution (case-insensitive)
        switch (fieldType.toLowerCase(Locale.ROOT)) {
            case "int8":
                return Schema.OPTIONAL_INT8_SCHEMA;
            case "int16":
                return Schema.OPTIONAL_INT16_SCHEMA;
            case "int32":
            case "integer":
                return Schema.OPTIONAL_INT32_SCHEMA;
            case "int64":
            case "long":
                return Schema.OPTIONAL_INT64_SCHEMA;
            case "float32":
            case "float":
                return Schema.OPTIONAL_FLOAT32_SCHEMA;
            case "float64":
            case "double":
                return Schema.OPTIONAL_FLOAT64_SCHEMA;
            case "boolean":
                return Schema.OPTIONAL_BOOLEAN_SCHEMA;
            case "string":
                return Schema.OPTIONAL_STRING_SCHEMA;
            case "bytes":
                return Schema.OPTIONAL_BYTES_SCHEMA;
            default:
                throw new ConfigException("Unsupported MongoDB mapping type: " + fieldType);
        }
    }

    private static Schema decimalSchema(Integer scale, Integer precision) {
        if (scale == null || scale < 0) {
            throw new ConfigException("Decimal requires an explicit non-negative scale");
        }
        final var builder = Decimal.builder(scale).optional();
        if (precision != null) {
            if (precision <= 0 || precision < scale) {
                throw new ConfigException("Decimal precision must be positive and at least scale");
            }
            builder.parameter("connect.decimal.precision", precision.toString());
        }
        return builder.build();
    }

    static Object convert(BsonValue value, Schema schema) {
        if (value == null || value.isNull()) {
            return null;
        }
        final String name = schema.name();
        if (name != null) {
            switch (name) {
                case Json.LOGICAL_NAME:
                    return json(value);
                case Uuid.LOGICAL_NAME:
                    if (value.isBinary()) {
                        return value.asBinary().asUuid().toString();
                    }
                    final String uuid = value.asString().getValue();
                    final String normalized = UUID.fromString(uuid).toString();
                    if (!normalized.equalsIgnoreCase(uuid)) {
                        throw new DataException("UUID strings must use the canonical hyphenated form");
                    }
                    return normalized;
                case Bits.LOGICAL_NAME:
                    final byte[] bits = value.asBinary().getData();
                    if (BitSet.valueOf(bits).length() > Integer.parseInt(schema.parameters().get(Bits.LENGTH_FIELD))) {
                        throw new DataException("Binary value exceeds the configured bit length");
                    }
                    return bits;
                case Decimal.LOGICAL_NAME:
                    final var decimal = number(value).setScale(Integer.parseInt(schema.parameters().get(Decimal.SCALE_FIELD)), RoundingMode.UNNECESSARY);
                    final String precision = schema.parameters().get("connect.decimal.precision");
                    if (precision != null && decimal.precision() > Integer.parseInt(precision)) {
                        throw new DataException("Decimal exceeds the configured precision");
                    }
                    return decimal;
                case VariableScaleDecimal.LOGICAL_NAME:
                    return VariableScaleDecimal.fromLogical(schema, number(value));
                case Date.LOGICAL_NAME:
                    return new java.util.Date(Math.multiplyExact(epochDay(value), 86_400_000L));
                case Time.LOGICAL_NAME:
                    return new java.util.Date(timeUnits(value, 1_000_000));
                case Timestamp.LOGICAL_NAME:
                    return new java.util.Date(timestampUnits(value, 1_000));
                case "io.debezium.time.Date":
                    return Math.toIntExact(epochDay(value));
                case "io.debezium.time.Timestamp":
                    return timestampUnits(value, 1_000);
                case "io.debezium.time.MicroTimestamp":
                    return timestampUnits(value, 1_000_000);
                case "io.debezium.time.NanoTimestamp":
                    return timestampUnits(value, 1_000_000_000);
                case "io.debezium.time.Time":
                    return Math.toIntExact(timeUnits(value, 1_000_000));
                case MicroTime.SCHEMA_NAME:
                    return timeUnits(value, 1_000);
                case NanoTime.SCHEMA_NAME:
                    return timeUnits(value, 1);
                case "io.debezium.time.ZonedTimestamp":
                    return value.isString() ? OffsetDateTime.parse(value.asString().getValue()).toString()
                            : instant(value).atOffset(ZoneOffset.UTC).toString();
                case ZonedTime.SCHEMA_NAME:
                    return value.isString() ? OffsetTime.parse(value.asString().getValue()).toString()
                            : instant(value).atOffset(ZoneOffset.UTC).toOffsetTime().toString();
                case Year.SCHEMA_NAME:
                    return value.isDateTime() ? instant(value).atOffset(ZoneOffset.UTC).getYear()
                            : java.time.Year.of(number(value).intValueExact()).getValue();
                case MicroDuration.SCHEMA_NAME:
                    if (value.isString()) {
                        final var duration = Duration.parse(value.asString().getValue());
                        return BigDecimal.valueOf(duration.getSeconds()).movePointRight(6)
                                .add(BigDecimal.valueOf(duration.getNano(), 3)).longValueExact();
                    }
                    return number(value).longValueExact();
                case NanoDuration.SCHEMA_NAME:
                    return value.isString() ? Duration.parse(value.asString().getValue()).toNanos() : number(value).longValueExact();
                default:
                    throw new DataException("Unsupported logical mapping type: " + name);
            }
        }
        return switch (schema.type()) {
            case INT8 -> number(value).byteValueExact();
            case INT16 -> number(value).shortValueExact();
            case INT32 -> number(value).intValueExact();
            case INT64 -> number(value).longValueExact();
            case FLOAT32 -> finiteFloat(number(value));
            case FLOAT64 -> finiteDouble(number(value));
            case BOOLEAN -> booleanValue(value);
            case STRING -> string(value);
            case BYTES -> value.asBinary().getData();
            default -> throw new DataException("Unsupported mapping type: " + schema.type());
        };
    }

    private static BigDecimal number(BsonValue value) {
        return switch (value.getBsonType()) {
            case INT32 -> BigDecimal.valueOf(value.asInt32().getValue());
            case INT64 -> BigDecimal.valueOf(value.asInt64().getValue());
            case DOUBLE -> BigDecimal.valueOf(value.asDouble().getValue());
            case DECIMAL128 -> value.asDecimal128().getValue().bigDecimalValue();
            case STRING -> new BigDecimal(value.asString().getValue());
            default -> throw new DataException("Expected a BSON number or numeric string");
        };
    }

    private static float finiteFloat(BigDecimal number) {
        final float result = number.floatValue();
        if (!Float.isFinite(result) || (result == 0 && number.signum() != 0)) {
            throw new DataException("Number is outside the FLOAT32 range");
        }
        return result;
    }

    private static double finiteDouble(BigDecimal number) {
        final double result = number.doubleValue();
        if (!Double.isFinite(result) || (result == 0 && number.signum() != 0)) {
            throw new DataException("Number is outside the FLOAT64 range");
        }
        return result;
    }

    private static boolean booleanValue(BsonValue value) {
        if (value.isBoolean()) {
            return value.asBoolean().getValue();
        }
        if (value.isString()) {
            final String text = value.asString().getValue();
            if ("true".equalsIgnoreCase(text) || "false".equalsIgnoreCase(text)) {
                return Boolean.parseBoolean(text);
            }
        }
        throw new DataException("Expected a BSON boolean or a 'true'/'false' string");
    }

    private static String string(BsonValue value) {
        return switch (value.getBsonType()) {
            case STRING -> value.asString().getValue();
            case OBJECT_ID -> value.asObjectId().getValue().toHexString();
            case INT32 -> Integer.toString(value.asInt32().getValue());
            case INT64 -> Long.toString(value.asInt64().getValue());
            case DOUBLE -> Double.toString(value.asDouble().getValue());
            case DECIMAL128 -> value.asDecimal128().getValue().toString();
            case BOOLEAN -> Boolean.toString(value.asBoolean().getValue());
            case DATE_TIME -> instant(value).toString();
            default -> throw new DataException("Expected a scalar value; use io.debezium.data.Json for BSON documents, arrays, or other BSON types");
        };
    }

    private static String json(BsonValue value) {
        // A wrapper lets the BSON writer serialize scalars and arrays as well as documents.
        final String wrapper = new BsonDocument("value", value).toJson(JSON_SETTINGS);
        return wrapper.substring(wrapper.indexOf(':') + 1, wrapper.length() - 1).trim();
    }

    private static Instant instant(BsonValue value) {
        if (value.isDateTime()) {
            return Instant.ofEpochMilli(value.asDateTime().getValue());
        }
        if (value.isTimestamp()) {
            // The increment orders operations within a second; it is not a fractional time.
            return Instant.ofEpochSecond(Integer.toUnsignedLong(value.asTimestamp().getTime()));
        }
        if (value.isString()) {
            return OffsetDateTime.parse(value.asString().getValue()).toInstant();
        }
        throw new DataException("Expected a BSON date, BSON timestamp, or ISO-8601 timestamp with an offset");
    }

    private static long timestampUnits(BsonValue value, long unitsPerSecond) {
        if (value.isNumber()) {
            return number(value).longValueExact();
        }
        final var instant = instant(value);
        final long nanosPerUnit = 1_000_000_000L / unitsPerSecond;
        if (instant.getNano() % nanosPerUnit != 0) {
            throw new DataException("Timestamp precision exceeds the configured type");
        }
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), unitsPerSecond), instant.getNano() / nanosPerUnit);
    }

    private static long epochDay(BsonValue value) {
        if (value.isNumber()) {
            return number(value).intValueExact();
        }
        if (value.isString()) {
            return LocalDate.parse(value.asString().getValue()).toEpochDay();
        }
        return instant(value).atOffset(ZoneOffset.UTC).toLocalDate().toEpochDay();
    }

    private static long timeUnits(BsonValue value, long nanosPerUnit) {
        if (value.isNumber()) {
            final long units = number(value).longValueExact();
            if (units < 0 || units >= 86_400_000_000_000L / nanosPerUnit) {
                throw new DataException("Time must be within one day");
            }
            return units;
        }
        final var time = value.isString() ? LocalTime.parse(value.asString().getValue())
                : instant(value).atOffset(ZoneOffset.UTC).toLocalTime();
        if (time.toNanoOfDay() % nanosPerUnit != 0) {
            throw new DataException("Time precision exceeds the configured type");
        }
        return time.toNanoOfDay() / nanosPerUnit;
    }
}
