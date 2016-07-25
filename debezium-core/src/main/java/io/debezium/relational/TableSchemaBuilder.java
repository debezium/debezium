/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.ThreadSafe;
import io.debezium.data.Bits;
import io.debezium.data.IsoTime;
import io.debezium.data.IsoTimestamp;
import io.debezium.data.SchemaUtil;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.TimeZoneAdapter;
import io.debezium.relational.mapping.ColumnMapper;
import io.debezium.relational.mapping.ColumnMappers;

/**
 * Builder that constructs {@link TableSchema} instances for {@link Table} definitions.
 * <p>
 * This builder is responsible for mapping {@link Column table columns} to {@link Field fields} in Kafka Connect {@link Schema}s,
 * and this is necessarily dependent upon the database's supported types. Although mappings are defined for standard types,
 * this class may need to be subclassed for each DBMS to add support for DBMS-specific types by overriding any of the
 * "{@code add*Field}" methods.
 * <p>
 * See the <a href="http://docs.oracle.com/javase/6/docs/technotes/guides/jdbc/getstart/mapping.html#table1">Java SE Mapping SQL
 * and Java Types</a> for details about how JDBC {@link Types types} map to Java value types.
 * 
 * @author Randall Hauch
 */
@ThreadSafe
@Immutable
public class TableSchemaBuilder {

    private static final Short SHORT_TRUE = new Short((short) 1);
    private static final Short SHORT_FALSE = new Short((short) 0);
    private static final Integer INTEGER_TRUE = new Integer(1);
    private static final Integer INTEGER_FALSE = new Integer(0);
    private static final Long LONG_TRUE = new Long(1L);
    private static final Long LONG_FALSE = new Long(0L);
    private static final Float FLOAT_TRUE = new Float(1.0);
    private static final Float FLOAT_FALSE = new Float(0.0);
    private static final Double DOUBLE_TRUE = new Double(1.0d);
    private static final Double DOUBLE_FALSE = new Double(0.0d);
    private static final Logger LOGGER = LoggerFactory.getLogger(TableSchemaBuilder.class);
    private static final LocalDate EPOCH_DAY = LocalDate.ofEpochDay(0);

    private final TimeZoneAdapter timeZoneAdapter;

    /**
     * Create a new instance of the builder that uses the {@link TimeZoneAdapter#create() default time zone adapter}.
     */
    public TableSchemaBuilder() {
        this(TimeZoneAdapter.create());
    }

    /**
     * Create a new instance of the builder.
     * @param timeZoneAdapter the adapter for temporal objects created by the source database; may not be null
     */
    public TableSchemaBuilder(TimeZoneAdapter timeZoneAdapter) {
        this.timeZoneAdapter = timeZoneAdapter;
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
        columns.forEach(column -> addField(schemaBuilder, column, null));
        Schema valueSchema = schemaBuilder.build();

        // And a generator that can be used to create values from rows in the result set ...
        TableId id = new TableId(null, null, name);
        Function<Object[], Struct> valueGenerator = createValueGenerator(valueSchema, id, columns, null, null);

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
     * @param schemaPrefix the prefix added to the table identifier to construct the schema names; may be null if there is no
     *            prefix
     * @param table the table definition; may not be null
     * @return the table schema that can be used for sending rows of data for this table to Kafka Connect; never null
     */
    public TableSchema create(String schemaPrefix, Table table) {
        return create(schemaPrefix, table, null, null);
    }

    /**
     * Create a {@link TableSchema} from the given {@link Table table definition}. The resulting TableSchema will have a
     * {@link TableSchema#keySchema() key schema} that contains all of the columns that make up the table's primary key,
     * and a {@link TableSchema#valueSchema() value schema} that contains only those columns that are not in the table's primary
     * key.
     * <p>
     * This is equivalent to calling {@code create(table,false)}.
     * 
     * @param schemaPrefix the prefix added to the table identifier to construct the schema names; may be null if there is no
     *            prefix
     * @param table the table definition; may not be null
     * @param filter the filter that specifies whether columns in the table should be included; may be null if all columns
     *            are to be included
     * @param mappers the mapping functions for columns; may be null if none of the columns are to be mapped to different values
     * @return the table schema that can be used for sending rows of data for this table to Kafka Connect; never null
     */
    public TableSchema create(String schemaPrefix, Table table, Predicate<ColumnId> filter, ColumnMappers mappers) {
        if (schemaPrefix == null) schemaPrefix = "";
        // Build the schemas ...
        final TableId tableId = table.id();
        final String tableIdStr = tableId.toString();
        final String schemaNamePrefix = schemaPrefix + tableIdStr;
        SchemaBuilder valSchemaBuilder = SchemaBuilder.struct().name(schemaNamePrefix + ".Value");
        SchemaBuilder keySchemaBuilder = SchemaBuilder.struct().name(schemaNamePrefix + ".Key");
        LOGGER.debug("Mapping table '{}' to schemas under '{}'", tableId, schemaNamePrefix);
        AtomicBoolean hasPrimaryKey = new AtomicBoolean(false);
        table.columns().forEach(column -> {
            if (table.isPrimaryKeyColumn(column.name())) {
                // The column is part of the primary key, so ALWAYS add it to the PK schema ...
                addField(keySchemaBuilder, column, null);
                hasPrimaryKey.set(true);
            }
            if (filter == null || filter.test(new ColumnId(tableId, column.name()))) {
                // Add the column to the value schema only if the column has not been filtered ...
                ColumnMapper mapper = mappers == null ? null : mappers.mapperFor(tableId, column);
                addField(valSchemaBuilder, column, mapper);
            }
        });
        Schema valSchema = valSchemaBuilder.optional().build();
        Schema keySchema = hasPrimaryKey.get() ? keySchemaBuilder.build() : null;

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Mapped primary key for table '{}' to schema: {}", tableId, SchemaUtil.asDetailedString(keySchema));
            LOGGER.debug("Mapped columns for table '{}' to schema: {}", tableId, SchemaUtil.asDetailedString(valSchema));
        }

        // Create the generators ...
        Function<Object[], Object> keyGenerator = createKeyGenerator(keySchema, tableId, table.primaryKeyColumns());
        Function<Object[], Struct> valueGenerator = createValueGenerator(valSchema, tableId, table.columns(), filter, mappers);

        // And the table schema ...
        return new TableSchema(keySchema, keyGenerator, valSchema, valueGenerator);
    }

    /**
     * Creates the function that produces a Kafka Connect key object for a row of data.
     * 
     * @param schema the Kafka Connect schema for the key; may be null if there is no known schema, in which case the generator
     *            will be null
     * @param columnSetName the name for the set of columns, used in error messages; may not be null
     * @param columns the column definitions for the table that defines the row; may not be null
     * @return the key-generating function, or null if there is no key schema
     */
    protected Function<Object[], Object> createKeyGenerator(Schema schema, TableId columnSetName, List<Column> columns) {
        if (schema != null) {
            int[] recordIndexes = indexesForColumns(columns);
            Field[] fields = fieldsForColumns(schema, columns);
            int numFields = recordIndexes.length;
            ValueConverter[] converters = convertersForColumns(schema, columnSetName, columns, null, null);
            return (row) -> {
                Struct result = new Struct(schema);
                for (int i = 0; i != numFields; ++i) {
                    Object value = row[recordIndexes[i]];
                    ValueConverter converter = converters[i];
                    if (converter != null) {
                        value = value == null ? value : converter.convert(value);
                        try {
                            result.put(fields[i], value);
                        } catch (DataException e) {
                            Column col = columns.get(i);
                            LOGGER.error("Failed to properly convert key value for '" + columnSetName + "." + col.name() + "' of type "
                                    + col.typeName() + ":", e);
                        }
                    }
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
     * @param tableId the table identifier; may not be null
     * @param columns the column definitions for the table that defines the row; may not be null
     * @param filter the filter that specifies whether columns in the table should be included; may be null if all columns
     *            are to be included
     * @param mappers the mapping functions for columns; may be null if none of the columns are to be mapped to different values
     * @return the value-generating function, or null if there is no value schema
     */
    protected Function<Object[], Struct> createValueGenerator(Schema schema, TableId tableId, List<Column> columns,
                                                              Predicate<ColumnId> filter, ColumnMappers mappers) {
        if (schema != null) {
            int[] recordIndexes = indexesForColumns(columns);
            Field[] fields = fieldsForColumns(schema, columns);
            int numFields = recordIndexes.length;
            ValueConverter[] converters = convertersForColumns(schema, tableId, columns, filter, mappers);
            AtomicBoolean traceMessage = new AtomicBoolean(true);
            return (row) -> {
                Struct result = new Struct(schema);
                for (int i = 0; i != numFields; ++i) {
                    Object value = row[recordIndexes[i]];
                    ValueConverter converter = converters[i];
                    if (converter != null) {
                        if (value != null) value = converter.convert(value);
                        try {
                            result.put(fields[i], value);
                        } catch (DataException e) {
                            Column col = columns.get(i);
                            LOGGER.error("Failed to properly convert data value for '" + tableId + "." + col.name() + "' of type "
                                    + col.typeName() + ":", e);
                        }
                    } else if (traceMessage.getAndSet(false)) {
                        Column col = columns.get(i);
                        LOGGER.trace("Excluding '" + tableId + "." + col.name() + "' of type " + col.typeName());
                    }
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
            Field field = schema.field(column.name()); // may be null if the field is unused ...
            fields[i.getAndIncrement()] = field;
        });
        return fields;
    }

    /**
     * Obtain the array of converters for each column in a row. A converter might be null if the column is not be included in
     * the records.
     * 
     * @param schema the schema; may not be null
     * @param tableId the identifier of the table that contains the columns
     * @param columns the columns in the row; may not be null
     * @param filter the filter that specifies whether columns in the table should be included; may be null if all columns
     *            are to be included
     * @param mappers the mapping functions for columns; may be null if none of the columns are to be mapped to different values
     * @return the converters for each column in the rows; never null
     */
    protected ValueConverter[] convertersForColumns(Schema schema, TableId tableId, List<Column> columns,
                                                    Predicate<ColumnId> filter, ColumnMappers mappers) {
        ValueConverter[] converters = new ValueConverter[columns.size()];
        AtomicInteger i = new AtomicInteger(0);
        columns.forEach(column -> {
            Field field = schema.field(column.name());
            ValueConverter converter = null;
            if (filter == null || filter.test(new ColumnId(tableId, column.name()))) {
                ValueConverter valueConverter = createValueConverterFor(column, field);
                assert valueConverter != null;
                if (mappers != null) {
                    ValueConverter mappingConverter = mappers.mappingConverterFor(tableId, column);
                    if (mappingConverter != null) {
                        converter = (value) -> {
                            if (value != null) value = valueConverter.convert(value);
                            return mappingConverter.convert(value);
                        };
                    }
                }
                if (converter == null) converter = valueConverter;
                assert converter != null;
            }
            converters[i.getAndIncrement()] = converter; // may be null if not included
        });
        return converters;
    }

    /**
     * Add to the supplied {@link SchemaBuilder} a field for the column with the given information.
     * 
     * @param builder the schema builder; never null
     * @param column the column definition
     * @param mapper the mapping function for the column; may be null if the columns is not to be mapped to different values
     */
    protected void addField(SchemaBuilder builder, Column column, ColumnMapper mapper) {
        SchemaBuilder fieldBuilder = null;
        switch (column.jdbcType()) {
            case Types.NULL:
                LOGGER.warn("Unexpected JDBC type: NULL");
                break;

            // Single- and multi-bit values ...
            case Types.BIT:
                if (column.length() > 1) {
                    fieldBuilder = Bits.builder();
                    break;
                }
                // otherwise, it is just one bit so use a boolean ...
            case Types.BOOLEAN:
                fieldBuilder = SchemaBuilder.bool();
                break;

            // Fixed-length binary values ...
            case Types.BLOB:
            case Types.BINARY:
                fieldBuilder = SchemaBuilder.bytes();
                break;

            // Variable-length binary values ...
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                fieldBuilder = SchemaBuilder.bytes();
                break;

            // Numeric integers
            case Types.TINYINT:
                // values are an 8-bit unsigned integer value between 0 and 255
                fieldBuilder = SchemaBuilder.int8();
                break;
            case Types.SMALLINT:
                // values are a 16-bit signed integer value between -32768 and 32767
                fieldBuilder = SchemaBuilder.int16();
                break;
            case Types.INTEGER:
                // values are a 32-bit signed integer value between - 2147483648 and 2147483647
                fieldBuilder = SchemaBuilder.int32();
                break;
            case Types.BIGINT:
                // values are a 64-bit signed integer value between -9223372036854775808 and 9223372036854775807
                fieldBuilder = SchemaBuilder.int64();
                break;

            // Numeric decimal numbers
            case Types.REAL:
                // values are single precision floating point number which supports 7 digits of mantissa.
                fieldBuilder = SchemaBuilder.float32();
                break;
            case Types.FLOAT:
            case Types.DOUBLE:
                // values are double precision floating point number which supports 15 digits of mantissa.
                fieldBuilder = SchemaBuilder.float64();
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                // values are fixed-precision decimal values with exact precision.
                // Use Kafka Connect's arbitrary precision decimal type and use the column's specified scale ...
                fieldBuilder = Decimal.builder(column.scale());
                break;

            // Fixed-length string values
            case Types.CHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NCLOB:
                fieldBuilder = SchemaBuilder.string();
                break;

            // Variable-length string values
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
            case Types.DATALINK:
            case Types.SQLXML:
                fieldBuilder = SchemaBuilder.string();
                break;

            // Date and time values
            case Types.DATE:
                fieldBuilder = Date.builder();
                break;
            case Types.TIME:
                fieldBuilder = Time.builder();
                break;
            case Types.TIMESTAMP:
                fieldBuilder = Timestamp.builder();
                break;
            case Types.TIME_WITH_TIMEZONE:
                fieldBuilder = IsoTime.builder();
                break;
            case Types.TIMESTAMP_WITH_TIMEZONE:
                fieldBuilder = IsoTimestamp.builder();
                break;

            // Other types ...
            case Types.ROWID:
                // often treated as a string, but we'll generalize and treat it as a byte array
                fieldBuilder = SchemaBuilder.bytes();
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
                fieldBuilder = addOtherField(column, mapper);
                break;
        }
        if (fieldBuilder != null) {
            if (mapper != null) {
                // Let the mapper add properties to the schema ...
                mapper.alterFieldSchema(column, fieldBuilder);
            }
            if (column.isOptional()) fieldBuilder.optional();
            builder.field(column.name(), fieldBuilder.build());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("- field '{}' ({}{}) from column {}", column.name(), builder.isOptional() ? "OPTIONAL " : "",
                             fieldBuilder.type(),
                             column);
            }
        }
    }

    /**
     * Return a {@link SchemaBuilder} for a field for the column with the given information.
     * <p>
     * Subclasses that wish to override or extend the mappings of JDBC/DBMS types to Kafka Connect value types can override
     * this method and delegate to this method before and/or after the custom logic. Similar behavior should be addressed
     * in a specialized {@link #addField(SchemaBuilder, Column, ColumnMapper)} as well.
     * 
     * @param column the column
     * @param mapper the mapping function for the column; may be null if the columns is not to be mapped to different values
     * @return the {@link SchemaBuilder} for the new field, ready to be {@link SchemaBuilder#build() build}; may be null
     */
    protected SchemaBuilder addOtherField(Column column, ColumnMapper mapper) {
        LOGGER.warn("Unexpected JDBC type: {}", column.jdbcType());
        return null;
    }

    /**
     * Create a {@link ValueConverter} that can be used to convert row values for the given column into the Kafka Connect value
     * object described by the {@link Field field definition}.
     * <p>
     * Subclasses can override this method to specialize the behavior. The subclass method should do custom checks and
     * conversions,
     * and then delegate to this method implementation to handle all other cases.
     * <p>
     * Alternatively, subclasses can leave this method as-is and instead override one of the lower-level type-specific methods
     * that this method calls (e.g., {@link #convertBinary(Column, Field, Object)},
     * {@link #convertTinyInt(Column, Field, Object)}, etc.).
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
                return (data) -> convertBit(column, fieldDefn, data);
            case Types.BOOLEAN:
                return (data) -> convertBoolean(column, fieldDefn, data);

            // Binary values ...
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return (data) -> convertBinary(column, fieldDefn, data);

            // Numeric integers
            case Types.TINYINT:
                return (data) -> convertTinyInt(column, fieldDefn, data);
            case Types.SMALLINT:
                return (data) -> convertSmallInt(column, fieldDefn, data);
            case Types.INTEGER:
                return (data) -> convertInteger(column, fieldDefn, data);
            case Types.BIGINT:
                return (data) -> convertBigInt(column, fieldDefn, data);

            // Numeric decimal numbers
            case Types.FLOAT:
                return (data) -> convertFloat(column, fieldDefn, data);
            case Types.DOUBLE:
                return (data) -> convertDouble(column, fieldDefn, data);
            case Types.REAL:
                return (data) -> convertReal(column, fieldDefn, data);
            case Types.NUMERIC:
                return (data) -> convertNumeric(column, fieldDefn, data);
            case Types.DECIMAL:
                return (data) -> convertDecimal(column, fieldDefn, data);

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
                return (data) -> convertString(column, fieldDefn, data);

            // Date and time values
            case Types.DATE:
                return (data) -> convertDate(column, fieldDefn, data);
            case Types.TIME:
                return (data) -> convertTime(column, fieldDefn, data);
            case Types.TIMESTAMP:
                return (data) -> convertTimestamp(column, fieldDefn, data);
            case Types.TIME_WITH_TIMEZONE:
                return (data) -> convertTimeWithZone(column, fieldDefn, data);
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return (data) -> convertTimestampWithZone(column, fieldDefn, data);

            // Other types ...
            case Types.ROWID:
                return (data) -> convertRowId(column, fieldDefn, data);

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
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertTimestampWithZone(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        OffsetDateTime dateTime = null;
        LOGGER.debug("TimestampWithZone: " + data + " , class=" + data.getClass());
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
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertTimeWithZone(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        OffsetTime time = null;
        LOGGER.debug("TimeWithZone: " + data + " , class=" + data.getClass());
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
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertTimestamp(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        java.util.Date date = null;
        LOGGER.debug("Timestamp: " + data + " , class=" + data.getClass());
        if (data instanceof java.util.Date) {
            ZonedDateTime zdt = timeZoneAdapter.toZonedDateTime((java.util.Date)data);
            date = java.util.Date.from(zdt.toInstant());
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
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertTime(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        java.util.Date date = null;
        LOGGER.debug("Time: " + data + " , class=" + data.getClass());
        if (data instanceof java.util.Date) {
            ZonedDateTime zdt = timeZoneAdapter.toZonedDateTime((java.util.Date)data);
            date = java.util.Date.from(zdt.toInstant());
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
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertDate(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        java.util.Date date = null;
        LOGGER.debug("Date: " + data + " , class=" + data.getClass());
        if (data instanceof java.util.Date) {
            ZonedDateTime zdt = timeZoneAdapter.toZonedDateTime((java.util.Date)data);
            date = java.util.Date.from(zdt.toInstant());
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

    /**
     * Converts a value object for an expected JDBC type of {@link Types#BLOB}, {@link Types#BINARY},
     * {@link Types#VARBINARY}, {@link Types#LONGVARBINARY}.
     * <p>
     * Per the JDBC specification, databases should return {@link java.sql.Date} instances that have no notion of time or
     * time zones. This method handles {@link java.sql.Date} objects plus any other standard date-related objects such as
     * {@link java.util.Date}, {@link java.time.LocalDate}, and {@link java.time.LocalDateTime}. If any of the types might
     * have time components, those time components are ignored.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertBinary(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        if (data instanceof char[]) {
            data = new String((char[]) data); // convert to string
        }
        if (data instanceof String) {
            // This was encoded as a hexadecimal string, but we receive it as a normal string ...
            data = ((String) data).getBytes();
        }
        if (data instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) data);
        }
        // An unexpected value
        return unexpectedBinary(data, fieldDefn);
    }

    /**
     * Handle the unexpected value from a row with a column type of {@link Types#BLOB}, {@link Types#BINARY},
     * {@link Types#VARBINARY}, {@link Types#LONGVARBINARY}.
     * 
     * @param value the binary value for which no conversion was found; never null
     * @param fieldDefn the field definition in the Kafka Connect schema; never null
     * @return the converted value, or null
     * @see #convertBinary(Column, Field, Object)
     */
    protected byte[] unexpectedBinary(Object value, Field fieldDefn) {
        LOGGER.warn("Unexpected JDBC BINARY value for field {} with schema {}: class={}, value={}", fieldDefn.name(),
                    fieldDefn.schema(), value.getClass(), value);
        return null;
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#TINYINT}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertTinyInt(Column column, Field fieldDefn, Object data) {
        return convertSmallInt(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#SMALLINT}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertSmallInt(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        if (data instanceof Short) return data;
        if (data instanceof Number) {
            Number value = (Number) data;
            return new Short(value.shortValue());
        }
        if (data instanceof Boolean) {
            return ((Boolean) data).booleanValue() ? SHORT_TRUE : SHORT_FALSE;
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#INTEGER}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertInteger(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        if (data instanceof Integer) return data;
        if (data instanceof Number) {
            Number value = (Number) data;
            return new Integer(value.intValue());
        }
        if (data instanceof Boolean) {
            return ((Boolean) data).booleanValue() ? INTEGER_TRUE : INTEGER_FALSE;
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#INTEGER}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertBigInt(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        if (data instanceof Long) return data;
        if (data instanceof Number) {
            Number value = (Number) data;
            return new Long(value.longValue());
        }
        if (data instanceof Boolean) {
            return ((Boolean) data).booleanValue() ? LONG_TRUE : LONG_FALSE;
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#FLOAT}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertFloat(Column column, Field fieldDefn, Object data) {
        return convertDouble(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#DOUBLE}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertDouble(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        if (data instanceof Double) return data;
        if (data instanceof Number) {
            Number value = (Number) data;
            return new Double(value.doubleValue());
        }
        if (data instanceof Boolean) {
            return ((Boolean) data).booleanValue() ? DOUBLE_TRUE : DOUBLE_FALSE;
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#REAL}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertReal(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        if (data instanceof Float) return data;
        if (data instanceof Number) {
            Number value = (Number) data;
            return new Float(value.floatValue());
        }
        if (data instanceof Boolean) {
            return ((Boolean) data).booleanValue() ? FLOAT_TRUE : FLOAT_FALSE;
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#NUMERIC}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertNumeric(Column column, Field fieldDefn, Object data) {
        BigDecimal decimal = null;
        if (data instanceof BigDecimal)
            decimal = (BigDecimal) data;
        else if (data instanceof Boolean)
            decimal = new BigDecimal(((Boolean) data).booleanValue() ? 1 : 0);
        else if (data instanceof Short)
            decimal = new BigDecimal(((Short) data).intValue());
        else if (data instanceof Integer)
            decimal = new BigDecimal(((Integer) data).intValue());
        else if (data instanceof Long)
            decimal = BigDecimal.valueOf(((Long) data).longValue());
        else if (data instanceof Float)
            decimal = BigDecimal.valueOf(((Float) data).doubleValue());
        else if (data instanceof Double)
            decimal = BigDecimal.valueOf(((Double) data).doubleValue());
        else {
            return handleUnknownData(column, fieldDefn, data);
        }
        return decimal;
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#NUMERIC}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertDecimal(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        BigDecimal decimal = null;
        if (data instanceof BigDecimal)
            decimal = (BigDecimal) data;
        else if (data instanceof Boolean)
            decimal = new BigDecimal(((Boolean) data).booleanValue() ? 1 : 0);
        else if (data instanceof Short)
            decimal = new BigDecimal(((Short) data).intValue());
        else if (data instanceof Integer)
            decimal = new BigDecimal(((Integer) data).intValue());
        else if (data instanceof Long)
            decimal = BigDecimal.valueOf(((Long) data).longValue());
        else if (data instanceof Float)
            decimal = BigDecimal.valueOf(((Float) data).doubleValue());
        else if (data instanceof Double)
            decimal = BigDecimal.valueOf(((Double) data).doubleValue());
        else {
            return handleUnknownData(column, fieldDefn, data);
        }
        return decimal;
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#CHAR}, {@link Types#VARCHAR},
     * {@link Types#LONGVARCHAR}, {@link Types#CLOB}, {@link Types#NCHAR}, {@link Types#NVARCHAR}, {@link Types#LONGNVARCHAR},
     * {@link Types#NCLOB}, {@link Types#DATALINK}, and {@link Types#SQLXML}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertString(Column column, Field fieldDefn, Object data) {
        return data == null ? null : data.toString();
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#ROWID}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertRowId(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        if (data instanceof java.sql.RowId) {
            java.sql.RowId row = (java.sql.RowId) data;
            return ByteBuffer.wrap(row.getBytes());
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#BIT}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertBit(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        if (data instanceof Boolean) return data;
        if (data instanceof Short) return ((Short) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        if (data instanceof Integer) return ((Integer) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        if (data instanceof Long) return ((Long) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        return handleUnknownData(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#BOOLEAN}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertBoolean(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        if (data instanceof Boolean) return data;
        if (data instanceof Short) return ((Short) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        if (data instanceof Integer) return ((Integer) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        if (data instanceof Long) return ((Long) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        return handleUnknownData(column, fieldDefn, data);
    }
}
