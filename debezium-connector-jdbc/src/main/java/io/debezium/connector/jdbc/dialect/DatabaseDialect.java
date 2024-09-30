/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.temporal.TemporalAccessor;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.engine.jdbc.Size;
import org.hibernate.query.NativeQuery;

import io.debezium.connector.jdbc.SinkRecordDescriptor;
import io.debezium.connector.jdbc.SinkRecordDescriptor.FieldDescriptor;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.connector.jdbc.relational.TableId;
import io.debezium.connector.jdbc.type.Type;

/**
 * Represents a dialect of SQL implemented by a particular RDBMS.
 *
 * Subclasses of this contract implement database-specific behavior, should be immutable,
 * and is capable of registering overrides to default behavior where applicable.
 *
 * @author Chris Cranford
 */
public interface DatabaseDialect {

    /**
     * Gets the dialect's database version.
     *
     * @return database version details
     */
    DatabaseVersion getVersion();

    /**
     * Resolves the table id for table name.
     *
     * @param tableName the table name.
     * @return the parsed table identifier, never {@code null}.
     */
    TableId getTableId(String tableName);

    /**
     * Check whether the specified table exists.
     *
     * @param connection the database connection to be used, should not be {@code null}.
     * @param tableId the table identifier, should not be {@code null}.
     * @return true if the table exists, false otherwise
     * @throws SQLException if a database exception occurs
     */
    boolean tableExists(Connection connection, TableId tableId) throws SQLException;

    /**
     * Read the table structure data from the database.
     *
     * @param connection the database connection to be used, should not be {@code null}.
     * @param tableId the table identifier, should not be {@code null}.
     * @return the table relational model if it exists
     * @throws SQLException if the table does not exist or a database exception occurs
     */
    TableDescriptor readTable(Connection connection, TableId tableId) throws SQLException;

    /**
     * Resolves what fields are missing from the provided table compared against the incoming record.
     *
     * @param record the current sink record being processed, should not be {@code null}
     * @param table the relational table model, should not be {@code null}
     * @return a collection of field names that are missing from the database table, can be {@code empty}.
     */
    Set<String> resolveMissingFields(SinkRecordDescriptor record, TableDescriptor table);

    /**
     * Construct a {@code CREATE TABLE} statement specific for this dialect based on the provided record.
     *
     * @param record the current sink record being processed, should not be {@code null}
     * @param tableId the tableidentifier to be used, should not be {@code null}
     * @return the create table SQL statement to be executed, never {@code null}
     */
    String getCreateTableStatement(SinkRecordDescriptor record, TableId tableId);

    /**
     * Gets the prefix used before adding column-clauses in {@code ALTER TABLE} statements.
     *
     * @return the alter table column-clauses prefix
     */
    String getAlterTablePrefix();

    /**
     * Gets the suffix used after adding the column-clauses in {@code ALTER TABLE} statements.
     *
     * @return the alter table column-clauses suffix
     */
    String getAlterTableSuffix();

    /**
     * Gets the prefix used before adding each column-clause to {@code ALTER TABLE} statements.
     *
     * @return the alter table prefix just before each column-clause
     */
    String getAlterTableColumnPrefix();

    /**
     * Gets the suffix used after adding each column-clause to {@code ALTER TABLE statements}.
     *
     * @return the alter table suffix just after each column-clause
     */
    String getAlterTableColumnSuffix();

    /**
     * Gets the field delimiter used when constructing {@code ALTER TABLE} statements.
     *
     * @return the field delimiter for alter table SQL statement
     */
    String getAlterTableColumnDelimiter();

    /**
     * Construct a {@code ALTER TABLE} statement specific for this dialect.
     *
     * @param table the current relational table model, should not be {@code null}
     * @param record the current sink record being processed, should not be {@code null}
     * @param missingFields the fields that have been determined as missing from the relational model, should not be {@code null}
     * @return the alter table SQL statement to be executed, never {@code null}
     * @throws IllegalArgumentException if called with an empty set of missing fields
     */
    String getAlterTableStatement(TableDescriptor table, SinkRecordDescriptor record, Set<String> missingFields);

    /**
     * Construct a {@code INSERT INTO} statement specific for this dialect.
     *
     * @param table the current relational table model, should not be {@code null}
     * @param record the current sink record being processed, should not be {@code null}
     * @return the insert SQL statement to be executed, never {@code null}
     */
    String getInsertStatement(TableDescriptor table, SinkRecordDescriptor record);

    /**
     * Construct a {@code UPSERT} statement specific for this dialect.
     *
     * @param table the current relational table model, should not be {@code null}
     * @param record the current sink record being processed, should not be {@code null}
     * @return the upsert SQL statement to be executed, never {@code null}
     */
    String getUpsertStatement(TableDescriptor table, SinkRecordDescriptor record);

    /**
     * Construct a {@code UPDATE} statement specific for this dialect.
     *
     * @param table the current relational table model, should not be {@code null}
     * @param record the current sink record being processed, should not be {@code null}
     * @return the update SQL statement to be executed, never {@code null}
     */
    String getUpdateStatement(TableDescriptor table, SinkRecordDescriptor record);

    /**
     * Construct a {@code DELETE} statement specific for this dialect.
     *
     * @param table the current relational table model, should not be {@code null}
     * @param record the current sink record being processed, should not be {@code null}
     * @return the delete SQL statement to be executed, never {@code null}
     */
    String getDeleteStatement(TableDescriptor table, SinkRecordDescriptor record);

    /**
     * Construct a {@code TRUNCATE} statement specific for this dialect.
     *
     * @param table the current relational table model, should not be {@code null}
     * @return the truncate SQL statement to be executed, never {@code null}
     */
    String getTruncateStatement(TableDescriptor table);

    /**
     * Returns the SQL binding fragment for a column, schema, and type mapping.
     *
     * @param column the relational column type, never {@code null}
     * @param schema the field schema type, never {@code null}
     * @param type the resolved field type, never {@code null}
     * @return the query binding SQL fragment
     */
    String getQueryBindingWithValueCast(ColumnDescriptor column, Schema schema, Type type);

    /**
     * Gets the maximum length of a VARCHAR field in a primary key column.
     *
     * @return maximum varchar field length when participating in the primary key
     */
    int getMaxVarcharLengthInKey();

    /**
     * Gets the maximum length of a nationalized VARCHAR field in a primary key column.
     *
     * @return maximum varchar field length when participating in the primary key
     */
    int getMaxNVarcharLengthInKey();

    /**
     * Gets the maximum length of a variable binary field in a primary key column.
     *
     * @return maximum field length when participating in the primary key
     */
    int getMaxVarbinaryLength();

    /**
     * Returns whether the user has specified a time zone JDBC property or whether the connector
     * configuration property {@code database.time_zone} has been specified.
     *
     * @return true if the properties have been specified; false otherwise.
     */
    boolean isTimeZoneSet();

    /**
     * Returns whether a time with time zone details be bound using the database time zone.
     *
     * @return true if the value should be shifted; false otherwise (the default).
     */
    boolean shouldBindTimeWithTimeZoneAsDatabaseTimeZone();

    /**
     * Gets the maximum precision allowed for a dialect's time data type.
     *
     * @return maximum time precision
     */
    default int getMaxTimePrecision() {
        return 6;
    }

    /**
     * Gets the maximum precision allowed for a dialect's timestamp data type.
     *
     * @return maximum timestamp precision
     */
    default int getMaxTimestampPrecision() {
        return 6;
    }

    /**
     * Get the default decimal data type precision for the dialect.
     *
     * @return default decimal precision
     */
    int getDefaultDecimalPrecision();

    /**
     * Get the default timestamp precision for the dialect.
     *
     * @return default timestamp precision
     */
    int getDefaultTimestampPrecision();

    /**
     * Returns whether the dialect permits negative scale.
     *
     * @return true if the dialect permits using negative scale values
     */
    boolean isNegativeScaleAllowed();

    default String getTimeQueryBinding() {
        return "?";
    }

    /**
     * Returns the default format for binding a byte array
     * @return the format for binding a byte array
     */
    String getByteArrayFormat();

    /**
     * Format a boolean.
     *
     * @param value the boolean value
     * @return the formatted string value
     */
    String getFormattedBoolean(boolean value);

    /**
     * Format a date.
     *
     * @param value the value to tbe formatted, never {@code null}
     * @return the formatted string value
     */
    String getFormattedDate(TemporalAccessor value);

    /**
     * Format a time.
     *
     * @param value the value to be formatted, never {@code null}
     * @return the formatted string value
     */
    String getFormattedTime(TemporalAccessor value);

    /**
     * Format a time with time zone.
     *
     * @param value the value to be formatted, never {@code null}
     * @return the formatted string value
     */
    String getFormattedTimeWithTimeZone(String value);

    /**
     * Format a date and time.
     *
     * @param value the value to be formatted, never {@code null}
     * @return the formatted string value
     */
    String getFormattedDateTime(TemporalAccessor value);

    /**
     * Format a date and time with nonoseconds.
     *
     * @param value the value to be formatted, never {@code null}
     * @return the formatted string value
     */
    String getFormattedDateTimeWithNanos(TemporalAccessor value);

    /**
     * Format a timestamp.
     *
     * @param value the value to be formatted, never {@code null}
     * @return the formatted string value
     */
    String getFormattedTimestamp(TemporalAccessor value);

    /**
     * Format a timestamp with time zone.
     *
     * @param value the value to be formatted, never {@code null}
     * @return the formatted string value.
     */
    String getFormattedTimestampWithTimeZone(String value);

    /**
     * Resolve the type for a given connect schema.
     *
     * @param schema connect schema, never {@code null}
     * @return resolved type to use
     */
    Type getSchemaType(Schema schema);

    /**
     * Resolves a JDBC type to a given SQL type name.
     *
     * @param jdbcType the JDBC type
     * @return the resolved type name
     */
    String getTypeName(int jdbcType);

    /**
     * Resolves a JDBC type with optional size parameters to a given SQL type name.
     *
     * @param jdbcType the JDBC type
     * @param size the optional size parameters, should not be {@code null}
     * @return the resolved type name
     */
    String getTypeName(int jdbcType, Size size);

    /**
     * Bind the specified value to the query.
     *
     * @param field the field being bound, should never be {@code null}
     * @param query the query the value is to be bound, should never be {@code null}
     * @param startIndex the starting index of the parameter binding
     * @param value the value to be bound, may be {@code null}
     * @return the next bind offset that should be used when binding multiple values
     */
    int bindValue(FieldDescriptor field, NativeQuery<?> query, int startIndex, Object value);
}
