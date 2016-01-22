/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.ThreadSafe;
import io.debezium.data.Bits;
import io.debezium.data.IsoTime;
import io.debezium.data.IsoTimestamp;
import io.debezium.jdbc.JdbcConnection;

/**
 * Builder that constructs {@link TableSchema} instances for {@link Table} definitions.
 * <p>
 * This builder is responsible for mapping {@link Column table columns} to {@link Field fields} in Kafka Connect {@link Schema}s,
 * and this is necessarily dependent upon the database's supported types. Although mappings are defined for standard types,
 * this class may need to be subclassed for each DBMS to add support for DBMS-specific types by overriding any of the
 * "{@code add*Field}" methods.
 * 
 * @author Randall Hauch
 */
@ThreadSafe
@Immutable
public class TableSchemaBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableSchemaBuilder.class);
    private static final LocalDate EPOCH_DAY = LocalDate.ofEpochDay(0);

    /**
     * Create a new instance of the builder.
     */
    public TableSchemaBuilder() {
    }

    /**
     * Create a {@link TableSchema} from the given JDBC {@link ResultSet}. The resulting TableSchema will have no primary key,
     * and its {@link TableSchema#valueSchema()} will contain fields for each column in the result set.
     * 
     * @param resultSet the result set for a query; may not be null
     * @param name the name of the value schema; may not be null
     * @return the table schema that can be used for sending rows of data for this table to Kafka Connect; never null
     * @throws SQLException if an error occurs while using the result set's metadata
     */
    public TableSchema create(ResultSet resultSet, String name) throws SQLException {
        // Determine the columns that make up the result set ...
        List<Column> columns = new ArrayList<>();
        JdbcConnection.columnsFor(resultSet, columns::add);

        // Create a schema that represents these columns ...
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(name);
        columns.forEach(column -> addField(schemaBuilder, column));
        Schema valueSchema = schemaBuilder.build();

        // And a generator that can be used to create values from rows in the result set ...
        Function<Object[], Struct> valueGenerator = createValueGenerator(valueSchema, columns);

        // Finally create our result object with no primary key or key generator ...
        return new TableSchema(null, null, valueSchema, valueGenerator);
    }

    /**
     * Create a {@link TableSchema} from the given {@link Table table definition}. The resulting TableSchema will have a
     * {@link TableSchema#keySchema() key schema} that contains all of the columns that make up the table's primary key,
     * and a {@link TableSchema#valueSchema() value schema} that contains only those columns that are not in the table's primary
     * key.
     * <p>
     * This is equivalent to calling {@code create(table,false)}.
     * 
     * @param table the table definition; may not be null
     * @return the table schema that can be used for sending rows of data for this table to Kafka Connect; never null
     * @see #create(Table, boolean)
     */
    public TableSchema create(Table table) {
        return create(table, false);
    }

    /**
     * Create a {@link TableSchema} from the given {@link Table table definition}. The resulting TableSchema will have a
     * {@link TableSchema#keySchema() key schema} that contains all of the columns that make up the table's primary key,
     * and a {@link TableSchema#valueSchema() value schema} that contains either all columns from the table or all columns
     * except those in the table's primary key.
     * 
     * @param table the table definition; may not be null
     * @param includePrimaryKeyColumnsInValue {@code false} if the primary key columns are to be excluded in the value
     *            {@link Schema} and generated {@link Struct}s, or {@code true} if all of the tables columns are to be included
     *            in the value {@link Schema} and generated {@link Struct}s
     * @return the table schema that can be used for sending rows of data for this table to Kafka Connect; never null
     * @see #create(Table)
     */
    public TableSchema create(Table table, boolean includePrimaryKeyColumnsInValue) {
        // Build the schemas ...
        SchemaBuilder valSchemaBuilder = SchemaBuilder.struct().name(table.id().toString());
        SchemaBuilder keySchemaBuilder = SchemaBuilder.struct().name(table.id().toString() + "/pk");
        AtomicBoolean hasPrimaryKey = new AtomicBoolean(false);
        table.columns().forEach(column -> {
            if (table.isPrimaryKeyColumn(column.name())) {
                // The column is part of the primary key, so add it to the PK schema ...
                addField(keySchemaBuilder, column);
                hasPrimaryKey.set(true);
                if (includePrimaryKeyColumnsInValue) {
                    // also add it to the value schema ...
                    addField(valSchemaBuilder, column);
                }
            } else {
                // The column is NOT part of the primary key, so add it to the value schema ...
                addField(valSchemaBuilder, column);
            }
        });
        Schema valSchema = valSchemaBuilder.build();
        Schema keySchema = hasPrimaryKey.get() ? keySchemaBuilder.build() : null;

        // Create the generators ...
        List<Column> valueColumns = includePrimaryKeyColumnsInValue ? table.columns() : table.nonPrimaryKeyColumns();
        Function<Object[], Object> keyGenerator = createKeyGenerator(keySchema, table.primaryKeyColumns());
        Function<Object[], Struct> valueGenerator = createValueGenerator(valSchema, valueColumns);

        // And the table schema ...
        return new TableSchema(keySchema, keyGenerator, valSchema, valueGenerator);
    }

    /**
     * Creates the function that produces a Kafka Connect key object for a row of data.
     * 
     * @param schema the Kafka Connect schema for the key; may be null if there is no known schema, in which case the generator
     *            will be null
     * @param columns the column definitions for the table that defines the row; may not be null
     * @return the key-generating function, or null if there is no key schema
     */
    protected Function<Object[], Object> createKeyGenerator(Schema schema, List<Column> columns) {
        if (schema != null) {
            int[] recordIndexes = indexesForColumns(columns);
            Field[] fields = fieldsForColumns(schema, columns);
            int numFields = recordIndexes.length;
            ValueConverter[] converters = convertersForColumns(schema, columns);
            return (row) -> {
                Struct result = new Struct(schema);
                for (int i = 0; i != numFields; ++i) {
                    Object value = row[recordIndexes[i]];
                    value = value == null ? value : converters[i].convert(value);
                    result.put(fields[i], value);
                }
                return result;
            };
        }
        return null;
    }

    /**
     * Creates the function that produces a Kafka Connect value object for a row of data.
     * 
     * @param schema the Kafka Connect schema for the value; may be null if there is no known schema, in which case the generator
     *            will be null
     * @param columns the column definitions for the table that defines the row; may not be null
     * @return the value-generating function, or null if there is no value schema
     */
    protected Function<Object[], Struct> createValueGenerator(Schema schema, List<Column> columns) {
        if (schema != null) {
            int[] recordIndexes = indexesForColumns(columns);
            Field[] fields = fieldsForColumns(schema, columns);
            int numFields = recordIndexes.length;
            ValueConverter[] converters = convertersForColumns(schema, columns);
            return (row) -> {
                Struct result = new Struct(schema);
                for (int i = 0; i != numFields; ++i) {
                    Object value = row[recordIndexes[i]];
                    value = value == null ? value : converters[i].convert(value);
                    result.put(fields[i], value);
                }
                return result;
            };
        }
        return null;
    }

    protected int[] indexesForColumns(List<Column> columns) {
        int[] recordIndexes = new int[columns.size()];
        AtomicInteger i = new AtomicInteger(0);
        columns.forEach(column -> {
            recordIndexes[i.getAndIncrement()] = column.position() - 1; // position is 1-based, indexes 0-based
        });
        return recordIndexes;
    }

    protected Field[] fieldsForColumns(Schema schema, List<Column> columns) {
        Field[] fields = new Field[columns.size()];
        AtomicInteger i = new AtomicInteger(0);
        columns.forEach(column -> {
            Field field = schema.field(column.name());
            assert field != null;
            fields[i.getAndIncrement()] = field;
        });
        return fields;
    }

    protected ValueConverter[] convertersForColumns(Schema schema, List<Column> columns) {
        ValueConverter[] converters = new ValueConverter[columns.size()];
        AtomicInteger i = new AtomicInteger(0);
        columns.forEach(column -> {
            Field field = schema.field(column.name());
            ValueConverter converter = createValueConverterFor(column, field);
            assert converter != null;
            converters[i.getAndIncrement()] = converter;
        });
        return converters;
    }

    /**
     * A function that converts from a column data value into a Kafka Connect object that is compliant with the Kafka Connect
     * {@link Schema}'s corresponding {@link Field}.
     */
    protected static interface ValueConverter {
        /**
         * Convert the column's data value into the Kafka Connect object.
         * 
         * @param data the column data value; never null
         * @return the Kafka Connect object
         */
        Object convert(Object data);
    }

    /**
     * Add to the supplied {@link SchemaBuilder} a field for the column with the given information.
     * 
     * @param builder the schema builder; never null
     * @param column the column definition
     */
    protected void addField(SchemaBuilder builder, Column column) {
        addField(builder, column.name(), column.jdbcType(), column.typeName(), column.length(), column.scale(), column.isOptional());
    }

    /**
     * Add to the supplied {@link SchemaBuilder} a field for the column with the given information.
     * <p>
     * Subclasses that wish to override or extend the mappings of JDBC/DBMS types to Kafka Connect value types can override
     * this method and delegate to this method before and/or after the custom logic. Similar behavior should be addressed
     * in a specialized {@link #createValueConverterFor(Column, Field)} as well.
     * 
     * @param builder the schema builder; never null
     * @param columnName the name of the column
     * @param jdbcType the column's {@link Types JDBC type}
     * @param typeName the column's DBMS-specific type name
     * @param columnLength the length of the column
     * @param columnScale the scale of the column values, or 0 if not a decimal value
     * @param optional {@code true} if the column is optional, or {@code false} if the column is known to always have a value
     */
    protected void addField(SchemaBuilder builder, String columnName, int jdbcType, String typeName, int columnLength,
                            int columnScale, boolean optional) {
        switch (jdbcType) {
            case Types.NULL:
                LOGGER.warn("Unexpected JDBC type: NULL");
                break;

            // Single- and multi-bit values ...
            case Types.BIT:
                if (columnLength > 1) {
                    SchemaBuilder bitBuilder = Bits.builder();
                    if (optional) bitBuilder.optional();
                    builder.field(columnName, bitBuilder.build());
                    break;
                }
                // otherwise, it is just one bit so use a boolean ...
            case Types.BOOLEAN:
                builder.field(columnName, optional ? Schema.OPTIONAL_BOOLEAN_SCHEMA : Schema.BOOLEAN_SCHEMA);
                break;

            // Binary values ...
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                builder.field(columnName, optional ? Schema.OPTIONAL_BYTES_SCHEMA : Schema.BYTES_SCHEMA);
                break;

            // Numeric integers
            case Types.TINYINT:
                // values are an 8-bit unsigned integer value between 0 and 255
                builder.field(columnName, optional ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA);
                break;
            case Types.SMALLINT:
                // values are a 16-bit signed integer value between -32768 and 32767
                builder.field(columnName, optional ? Schema.OPTIONAL_INT16_SCHEMA : Schema.INT16_SCHEMA);
                break;
            case Types.INTEGER:
                // values are a 32-bit signed integer value between - 2147483648 and 2147483647
                builder.field(columnName, optional ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA);
                break;
            case Types.BIGINT:
                // values are a 64-bit signed integer value between -9223372036854775808 and 9223372036854775807
                builder.field(columnName, optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA);
                break;

            // Numeric decimal numbers
            case Types.REAL:
                // values are single precision floating point number which supports 7 digits of mantissa.
                builder.field(columnName, optional ? Schema.OPTIONAL_FLOAT32_SCHEMA : Schema.FLOAT32_SCHEMA);
                break;
            case Types.FLOAT:
            case Types.DOUBLE:
                // values are double precision floating point number which supports 15 digits of mantissa.
                builder.field(columnName, optional ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.OPTIONAL_FLOAT64_SCHEMA);
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                // values are fixed-precision decimal values with exact precision.
                // Use Kafka Connect's arbitrary precision decimal type and use the column's specified scale ...
                SchemaBuilder decBuilder = Decimal.builder(columnScale);
                if (optional) decBuilder.optional();
                builder.field(columnName, decBuilder.build());
                break;

            // String values
            case Types.CHAR: // variable-length
            case Types.VARCHAR: // variable-length
            case Types.LONGVARCHAR: // variable-length
            case Types.CLOB: // variable-length
            case Types.NCHAR: // fixed-length
            case Types.NVARCHAR: // fixed-length
            case Types.LONGNVARCHAR: // fixed-length
            case Types.NCLOB: // fixed-length
            case Types.DATALINK:
            case Types.SQLXML:
                builder.field(columnName, optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
                break;

            // Date and time values
            case Types.DATE:
                SchemaBuilder dateBuilder = Date.builder();
                if (optional) dateBuilder.optional();
                builder.field(columnName, dateBuilder.build());
                break;
            case Types.TIME:
                SchemaBuilder timeBuilder = Time.builder();
                if (optional) timeBuilder.optional();
                builder.field(columnName, timeBuilder.build());
                break;
            case Types.TIMESTAMP:
                SchemaBuilder timestampBuilder = Timestamp.builder();
                if (optional) timestampBuilder.optional();
                builder.field(columnName, timestampBuilder.build());
                break;
            case Types.TIME_WITH_TIMEZONE:
                SchemaBuilder offsetTimeBuilder = IsoTime.builder();
                if (optional) offsetTimeBuilder.optional();
                builder.field(columnName, offsetTimeBuilder.build());
                break;
            case Types.TIMESTAMP_WITH_TIMEZONE:
                SchemaBuilder tsWithTzBuilder = IsoTimestamp.builder();
                if (optional) tsWithTzBuilder.optional();
                builder.field(columnName, tsWithTzBuilder.build());
                break;

            // Other types ...
            case Types.ROWID:
                // often treated as a string, but we'll generalize and treat it as a byte array
                builder.field(columnName, optional ? Schema.OPTIONAL_BYTES_SCHEMA : Schema.BYTES_SCHEMA);
                break;

            // Unhandled types
            case Types.ARRAY:
            case Types.DISTINCT:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.REF:
            case Types.REF_CURSOR:
            case Types.STRUCT:
            default:
                addOtherField(builder, columnName, jdbcType, typeName, columnLength, columnScale, optional);
                break;
        }
    }

    /**
     * Add to the supplied {@link SchemaBuilder} a field for the column with the given information.
     * <p>
     * Subclasses that wish to override or extend the mappings of JDBC/DBMS types to Kafka Connect value types can override
     * this method and delegate to this method before and/or after the custom logic. Similar behavior should be addressed
     * in a specialized {@link #addField(SchemaBuilder, String, int, String, int, int, boolean)} as well.
     * 
     * @param builder the schema builder; never null
     * @param columnName the name of the column
     * @param jdbcType the column's {@link Types JDBC type}
     * @param typeName the column's DBMS-specific type name
     * @param columnLength the length of the column
     * @param columnScale the scale of the column values, or 0 if not a decimal value
     * @param optional {@code true} if the column is optional, or {@code false} if the column is known to always have a value
     */
    protected void addOtherField(SchemaBuilder builder, String columnName, int jdbcType, String typeName, int columnLength,
                                 int columnScale, boolean optional) {
        LOGGER.warn("Unexpected JDBC type: {}", jdbcType);
    }

    /**
     * Create a {@link ValueConverter} that can be used to convert row values for the given column into the Kafka Connect value
     * object described by the {@link Field field definition}.
     * 
     * @param column the column describing the input values; never null
     * @param fieldDefn the definition for the field in a Kafka Connect {@link Schema} describing the output of the function;
     *            never null
     * @return the value conversion function; may not be null
     */
    protected ValueConverter createValueConverterFor(Column column, Field fieldDefn) {
        switch (column.jdbcType()) {
            case Types.NULL:
                return (data) -> null;
            case Types.BIT:
            case Types.BOOLEAN:
                return (data) -> {
                    if ( data instanceof Boolean ) return (Boolean)data;
                    if ( data instanceof Short ) return ((Short)data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
                    if ( data instanceof Integer ) return ((Integer)data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
                    if ( data instanceof Long ) return ((Long)data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
                    return handleUnknownData(column, fieldDefn, data);
                };

            // Binary values ...
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return (data) -> (byte[]) data;

            // Numeric integers
            case Types.TINYINT:
                return (data) -> {
                    if (data instanceof Byte) return (Byte)data;
                    if (data instanceof Boolean) return ((Boolean) data).booleanValue() ? (byte) 1 : (byte) 0;
                    return handleUnknownData(column, fieldDefn, data);
                };
            case Types.SMALLINT:
                return (data) -> {
                    if (data instanceof Short)  return (Short)data;
                    if (data instanceof Integer)  return new Short(((Integer)data).shortValue());
                    if (data instanceof Long)  return new Short(((Long)data).shortValue());
                    return handleUnknownData(column, fieldDefn, data);
                };
            case Types.INTEGER:
                return (data) -> {
                    if (data instanceof Integer)  return (Integer)data;
                    if (data instanceof Short)  return new Integer(((Short)data).intValue());
                    if (data instanceof Long)  return new Integer(((Long)data).intValue());
                    return handleUnknownData(column, fieldDefn, data);
                };
            case Types.BIGINT:
                return (data) -> {
                    if (data instanceof Long)  return (Long)data;
                    if (data instanceof Integer)  return new Long(((Integer)data).longValue());
                    if (data instanceof Short)  return new Long(((Short)data).longValue());
                    return handleUnknownData(column, fieldDefn, data);
                };

            // Numeric decimal numbers
            case Types.REAL:
            case Types.DOUBLE:
                return (data) -> {
                    if (data instanceof Double)  return (Double)data;
                    if (data instanceof Float)  return new Double(((Float)data).doubleValue());
                    if (data instanceof Integer)  return new Double(((Integer)data).doubleValue());
                    if (data instanceof Long)  return new Double(((Long)data).doubleValue());
                    if (data instanceof Short)  return new Double(((Short)data).doubleValue());
                    return handleUnknownData(column, fieldDefn, data);
                };
            case Types.FLOAT:
                return (data) -> {
                    if (data instanceof Float)  return (Float)data;
                    if (data instanceof Double)  return new Float(((Double)data).floatValue());
                    if (data instanceof Integer)  return new Float(((Integer)data).floatValue());
                    if (data instanceof Long)  return new Float(((Long)data).floatValue());
                    if (data instanceof Short)  return new Float(((Short)data).floatValue());
                    return handleUnknownData(column, fieldDefn, data);
                };
            case Types.NUMERIC:
            case Types.DECIMAL:
                return (data) -> {
                    BigDecimal decimal = null;
                    if ( data instanceof BigDecimal) decimal = (BigDecimal)data;
                    else if (data instanceof Boolean) decimal = new BigDecimal(((Boolean)data).booleanValue() ? 1 : 0);
                    else if (data instanceof Short) decimal = new BigDecimal(((Short)data).intValue());
                    else if (data instanceof Integer) decimal = new BigDecimal(((Integer)data).intValue());
                    else if (data instanceof Long) decimal = BigDecimal.valueOf(((Long)data).longValue());
                    else if (data instanceof Float) decimal = BigDecimal.valueOf(((Float)data).doubleValue());
                    else if (data instanceof Double) decimal = BigDecimal.valueOf(((Double)data).doubleValue());
                    else {
                        handleUnknownData(column, fieldDefn, data);
                    }
                    return decimal;
                };

            // String values
            case Types.CHAR: // variable-length
            case Types.VARCHAR: // variable-length
            case Types.LONGVARCHAR: // variable-length
            case Types.CLOB: // variable-length
            case Types.NCHAR: // fixed-length
            case Types.NVARCHAR: // fixed-length
            case Types.LONGNVARCHAR: // fixed-length
            case Types.NCLOB: // fixed-length
            case Types.DATALINK:
            case Types.SQLXML:
                return (data) -> data.toString();

            // Date and time values
            case Types.DATE:
                return (data) -> convertDate(fieldDefn, data);
            case Types.TIME:
                return (data) -> convertTime(fieldDefn, data);
            case Types.TIMESTAMP:
                return (data) -> convertTimestamp(fieldDefn, data);
            case Types.TIME_WITH_TIMEZONE:
                return (data) -> convertTimeWithZone(fieldDefn, data);
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return (data) -> convertTimestampWithZone(fieldDefn, data);

            // Other types ...
            case Types.ROWID:
                return (data) -> {
                    java.sql.RowId rowId = (java.sql.RowId) data;
                    return rowId.getBytes();
                };

            // Unhandled types
            case Types.ARRAY:
            case Types.DISTINCT:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.REF:
            case Types.REF_CURSOR:
            case Types.STRUCT:
            default:
                // An unexpected type of data ...
                return (data) -> handleUnknownData(column, fieldDefn, data);
        }
    }

    /**
     * Convert a value object from the specified column to the value type described by the specified Kafka Connect {@link Field
     * field}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object handleUnknownData(Column column, Field fieldDefn, Object data) {
        LOGGER.warn("Unexpected value for JDBC type {} and column {}: class={}, value={}", column.jdbcType(), column,
                    data.getClass(), data);
        return null;
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#TIMESTAMP_WITH_TIMEZONE}.
     * The <a href="http://www.oracle.com/technetwork/articles/java/jf14-date-time-2125367.html">standard ANSI to Java 8 type
     * mappings</a> specify that the preferred mapping (when using JDBC's {@link java.sql.ResultSet#getObject(int) getObject(...)}
     * methods) in Java 8 is to return {@link OffsetDateTime} for these values.
     * <p>
     * This method handles several types of objects, including {@link OffsetDateTime}, {@link java.sql.Timestamp},
     * {@link java.util.Date}, {@link java.time.LocalTime}, and {@link java.time.LocalDateTime}.
     * 
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertTimestampWithZone(Field fieldDefn, Object data) {
        OffsetDateTime dateTime = null;
        if (data instanceof OffsetDateTime) {
            // JDBC specification indicates that this will be the canonical object for this JDBC type.
            dateTime = (OffsetDateTime) data;
        } else if (data instanceof java.sql.Timestamp) {
            // Contains only time info without TZ information, so we assume UTC ...
            java.sql.Timestamp sqlTime = (java.sql.Timestamp) data;
            dateTime = sqlTime.toInstant().atOffset(ZoneOffset.UTC);
        } else if (data instanceof java.sql.Date) {
            // Contains only date info without TZ information, so we assume UTC ...
            java.sql.Time sqlTime = (java.sql.Time) data;
            dateTime = sqlTime.toInstant().atOffset(ZoneOffset.UTC);
        } else if (data instanceof java.util.Date) {
            // Contains date and time info without TZ information, so we assume UTC ...
            java.util.Date date = (java.util.Date) data;
            dateTime = date.toInstant().atOffset(ZoneOffset.UTC);
        } else if (data instanceof java.time.LocalDate) {
            // Contains date and time info without TZ information, so we assume UTC ...
            java.time.LocalDate local = (java.time.LocalDate) data;
            dateTime = local.atTime(OffsetTime.of(0, 0, 0, 0, ZoneOffset.UTC));
        } else if (data instanceof java.time.LocalDateTime) {
            // Contains local date and time with no TZ info, so assume UTC ...
            java.time.LocalDateTime local = (java.time.LocalDateTime) data;
            dateTime = local.atOffset(ZoneOffset.UTC);
        } else {
            // An unexpected
            dateTime = unexpectedTimestampWithZone(data, fieldDefn);
        }
        return dateTime;
    }

    /**
     * Handle the unexpected value from a row with a column type of {@link Types#TIMESTAMP_WITH_TIMEZONE}.
     * 
     * @param value the timestamp value for which no conversion was found; never null
     * @param fieldDefn the field definition in the Kafka Connect schema; never null
     * @return the converted value, or null
     */
    protected OffsetDateTime unexpectedTimestampWithZone(Object value, Field fieldDefn) {
        LOGGER.warn("Unexpected JDBC TIMESTAMP_WITH_TIMEZONE value for field {} with schema {}: class={}, value={}", fieldDefn.name(),
                    fieldDefn.schema(), value.getClass(), value);
        return null;
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#TIME_WITH_TIMEZONE}.
     * The <a href="http://www.oracle.com/technetwork/articles/java/jf14-date-time-2125367.html">standard ANSI to Java 8 type
     * mappings</a> specify that the preferred mapping (when using JDBC's {@link java.sql.ResultSet#getObject(int) getObject(...)}
     * methods) in Java 8 is to return {@link OffsetTime} for these values.
     * <p>
     * This method handles several types of objects, including {@link OffsetTime}, {@link java.sql.Time}, {@link java.util.Date},
     * {@link java.time.LocalTime}, and {@link java.time.LocalDateTime}. If any of the types have date components, those date
     * components are ignored.
     * 
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertTimeWithZone(Field fieldDefn, Object data) {
        OffsetTime time = null;
        if (data instanceof OffsetTime) {
            // JDBC specification indicates that this will be the canonical object for this JDBC type.
            time = (OffsetTime) data;
        } else if (data instanceof java.sql.Time) {
            // Contains only time info without TZ information, so we assume UTC ...
            java.sql.Time sqlTime = (java.sql.Time) data;
            time = OffsetTime.of(sqlTime.toLocalTime(), ZoneOffset.UTC);
        } else if (data instanceof java.util.Date) {
            // Contains time (without TZ) info and possibly date info, although we will ignore the date details.
            java.util.Date date = (java.util.Date) data;
            time = OffsetTime.ofInstant(date.toInstant(), ZoneOffset.UTC.normalized());
        } else if (data instanceof java.time.LocalTime) {
            // Contains local time with no TZ info, so assume UTC ...
            java.time.LocalTime local = (java.time.LocalTime) data;
            time = local.atOffset(ZoneOffset.UTC);
        } else if (data instanceof java.time.LocalDateTime) {
            // Contains local date and time with no TZ info, so assume UTC ...
            java.time.LocalDateTime local = (java.time.LocalDateTime) data;
            time = local.toLocalTime().atOffset(ZoneOffset.UTC);
        } else {
            // An unexpected
            time = unexpectedTimeWithZone(data, fieldDefn);
        }
        return time;
    }

    /**
     * Handle the unexpected value from a row with a column type of {@link Types#TIME_WITH_TIMEZONE}.
     * 
     * @param value the timestamp value for which no conversion was found; never null
     * @param fieldDefn the field definition in the Kafka Connect schema; never null
     * @return the converted value, or null
     */
    protected OffsetTime unexpectedTimeWithZone(Object value, Field fieldDefn) {
        LOGGER.warn("Unexpected JDBC TIME_WITH_TIMEZONE value for field {} with schema {}: class={}, value={}", fieldDefn.name(),
                    fieldDefn.schema(), value.getClass(), value);
        return null;
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#TIMESTAMP}.
     * <p>
     * Per the JDBC specification, databases should return {@link java.sql.Timestamp} instances, which have date and time info
     * but no time zone info. This method handles {@link java.sql.Date} objects plus any other standard date-related objects such
     * as {@link java.util.Date}, {@link java.time.LocalTime}, and {@link java.time.LocalDateTime}.
     * 
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertTimestamp(Field fieldDefn, Object data) {
        java.util.Date date = null;
        if (data instanceof java.sql.Timestamp) {
            // JDBC specification indicates that this will be the canonical object for this JDBC type.
            date = (java.util.Date) data;
        } else if (data instanceof java.sql.Date) {
            // This should still work, even though it should have just date info
            date = (java.util.Date) data;
        } else if (data instanceof java.util.Date) {
            // Possible that some implementations might use this.
            date = (java.util.Date) data;
        } else if (data instanceof java.time.LocalDate) {
            // If we get a local date (no TZ info), we need to just convert to a util.Date (no TZ info) ...
            java.time.LocalDate local = (java.time.LocalDate) data;
            date = java.util.Date.from(local.atStartOfDay().toInstant(ZoneOffset.UTC));
        } else if (data instanceof java.time.LocalDateTime) {
            // Get the instant in time by changing any date info to the epoch day so we only have time ...
            java.time.LocalDateTime local = (java.time.LocalDateTime) data;
            date = java.util.Date.from(local.toInstant(ZoneOffset.UTC));
        } else {
            // An unexpected
            date = unexpectedTimestamp(data, fieldDefn);
        }
        return date;
    }

    /**
     * Handle the unexpected value from a row with a column type of {@link Types#TIMESTAMP}.
     * 
     * @param value the timestamp value for which no conversion was found; never null
     * @param fieldDefn the field definition in the Kafka Connect schema; never null
     * @return the converted value, or null
     */
    protected java.util.Date unexpectedTimestamp(Object value, Field fieldDefn) {
        LOGGER.warn("Unexpected JDBC TIMESTAMP value for field {} with schema {}: class={}, value={}", fieldDefn.name(),
                    fieldDefn.schema(), value.getClass(), value);
        return null;
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#TIME}.
     * <p>
     * Per the JDBC specification, databases should return {@link java.sql.Time} instances that have no notion of date or
     * time zones. This method handles {@link java.sql.Date} objects plus any other standard date-related objects such as
     * {@link java.util.Date}, {@link java.time.LocalTime}, and {@link java.time.LocalDateTime}. If any of the types might
     * have date components, those date components are ignored.
     * 
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertTime(Field fieldDefn, Object data) {
        java.util.Date date = null;
        if (data instanceof java.sql.Time) {
            // JDBC specification indicates that this will be the canonical object for this JDBC type.
            // Contains only time info, with the date set to the epoch day ...
            date = (java.sql.Date) data;
        } else if (data instanceof java.util.Date) {
            // Possible that some implementations might use this. We ignore any date info by converting to an
            // instant and changing the date to the epoch date, and finally creating a new java.util.Date ...
            date = (java.util.Date) data;
            Instant instant = Instant.ofEpochMilli(date.getTime()).with(ChronoField.EPOCH_DAY, 0);
            date = new java.util.Date(instant.toEpochMilli());
        } else if (data instanceof java.time.LocalTime) {
            // If we get a local time (no TZ info), we need to just convert to a util.Date (no TZ info) ...
            java.time.LocalTime local = (java.time.LocalTime) data;
            date = java.util.Date.from(local.atDate(EPOCH_DAY).toInstant(ZoneOffset.UTC));
        } else if (data instanceof java.time.LocalDateTime) {
            // Get the instant in time by changing any date info to the epoch day so we only have time ...
            java.time.LocalDateTime local = (java.time.LocalDateTime) data;
            date = java.util.Date.from(local.with(ChronoField.EPOCH_DAY, 0).toInstant(ZoneOffset.UTC));
        } else {
            // An unexpected
            date = unexpectedTime(data, fieldDefn);
        }
        return date;
    }

    /**
     * Handle the unexpected value from a row with a column type of {@link Types#TIME}.
     * 
     * @param value the timestamp value for which no conversion was found; never null
     * @param fieldDefn the field definition in the Kafka Connect schema; never null
     * @return the converted value, or null
     */
    protected java.util.Date unexpectedTime(Object value, Field fieldDefn) {
        LOGGER.warn("Unexpected JDBC TIME value for field {} with schema {}: class={}, value={}", fieldDefn.name(),
                    fieldDefn.schema(), value.getClass(), value);
        return null;
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#DATE}.
     * <p>
     * Per the JDBC specification, databases should return {@link java.sql.Date} instances that have no notion of time or
     * time zones. This method handles {@link java.sql.Date} objects plus any other standard date-related objects such as
     * {@link java.util.Date}, {@link java.time.LocalDate}, and {@link java.time.LocalDateTime}. If any of the types might
     * have time components, those time components are ignored.
     * 
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertDate(Field fieldDefn, Object data) {
        java.util.Date date = null;
        if (data instanceof java.sql.Date) {
            // JDBC specification indicates that this will be the nominal object for this JDBC type.
            // Contains only date info, with all time values set to all zeros (e.g. midnight)
            date = (java.sql.Date) data;
        } else if (data instanceof java.util.Date) {
            // Possible that some implementations might use this. We should be prepared to ignore any time,
            // information by truncating to days and creating a new java.util.Date ...
            date = (java.util.Date) data;
            Instant instant = Instant.ofEpochMilli(date.getTime()).truncatedTo(ChronoUnit.DAYS);
            date = new java.util.Date(instant.toEpochMilli());
        } else if (data instanceof java.time.LocalDate) {
            // If we get a local date (no TZ info), we need to just convert to a util.Date (no TZ info) ...
            java.time.LocalDate local = (java.time.LocalDate) data;
            date = java.util.Date.from(local.atStartOfDay().toInstant(ZoneOffset.UTC));
        } else if (data instanceof java.time.LocalDateTime) {
            // Get the instant with no time component ...
            java.time.LocalDateTime local = (java.time.LocalDateTime) data;
            date = java.util.Date.from(local.truncatedTo(ChronoUnit.DAYS).toInstant(ZoneOffset.UTC));
        } else {
            // An unexpected
            date = unexpectedDate(data, fieldDefn);
        }
        return date;
    }

    /**
     * Handle the unexpected value from a row with a column type of {@link Types#DATE}.
     * 
     * @param value the timestamp value for which no conversion was found; never null
     * @param fieldDefn the field definition in the Kafka Connect schema; never null
     * @return the converted value, or null
     */
    protected java.util.Date unexpectedDate(Object value, Field fieldDefn) {
        LOGGER.warn("Unexpected JDBC DATE value for field {} with schema {}: class={}, value={}", fieldDefn.name(),
                    fieldDefn.schema(), value.getClass(), value);
        return null;
    }
}
