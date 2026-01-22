/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.db2i;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Optional;

import org.hibernate.SessionFactory;
import org.hibernate.dialect.DB2iDialect;
import org.hibernate.dialect.Dialect;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkRecord;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.GeneralDatabaseDialect;
import io.debezium.connector.jdbc.dialect.SqlStatementBuilder;
import io.debezium.connector.jdbc.dialect.db2i.connect.ConnectDateType;
import io.debezium.connector.jdbc.dialect.db2i.connect.ConnectTimeType;
import io.debezium.connector.jdbc.dialect.db2i.connect.ConnectTimestampType;
import io.debezium.connector.jdbc.dialect.db2i.debezium.MicroTimeType;
import io.debezium.connector.jdbc.dialect.db2i.debezium.MicroTimestampType;
import io.debezium.connector.jdbc.dialect.db2i.debezium.NanoTimeType;
import io.debezium.connector.jdbc.dialect.db2i.debezium.NanoTimestampType;
import io.debezium.connector.jdbc.dialect.db2i.debezium.TimeType;
import io.debezium.connector.jdbc.dialect.db2i.debezium.TimestampType;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.field.FieldDescriptor;
import io.debezium.time.ZonedTimestamp;

/**
 * A {@link DatabaseDialect} implementation for Db2i.
 *
 * @author Andrew Love
 */
public class Db2iDatabaseDialect extends GeneralDatabaseDialect {

    private static final DateTimeFormatter ISO_LOCAL_DATE_TIME_WITH_SPACE = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .toFormatter();

    public static class Db2iDatabaseProvider implements DatabaseDialectProvider {
        @Override
        public boolean supports(Dialect dialect) {
            return dialect instanceof DB2iDialect;
        }

        @Override
        public Class<?> name() {
            return Db2iDatabaseDialect.class;
        }

        @Override
        public DatabaseDialect instantiate(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
            return new Db2iDatabaseDialect(config, sessionFactory);
        }
    }

    private Db2iDatabaseDialect(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
        super(config, sessionFactory);
    }

    @Override
    protected Optional<String> getDatabaseTimeZoneQuery() {
        return Optional.of("SELECT CURRENT TIMEZONE FROM sysibm.sysdummy1");
    }

    @Override
    protected String columnQueryBindingFromField(String fieldName, TableDescriptor table, JdbcSinkRecord record) {
        final String binding = super.columnQueryBindingFromField(fieldName, table, record);

        // DB2i requires explicit CAST for parameter markers in MERGE statements
        if (!"?".equals(binding)) {
            return binding;
        }

        final FieldDescriptor field = record.allFields().get(fieldName);
        final String columnName = resolveColumnName(field);
        final ColumnDescriptor column = table.getColumnByName(columnName);

        String typeName = column.getTypeName();
        if (typeName == null || typeName.isEmpty()) {
            // Fallback if type name is not available
            return binding;
        }

        // DB2i requires size specification in CAST, append precision/scale if not already present
        if (!typeName.contains("(")) {
            final int precision = column.getPrecision();
            final int scale = column.getScale();

            if (precision > 0) {
                typeName = (scale > 0)
                        ? typeName + "(" + precision + "," + scale + ")"
                        : typeName + "(" + precision + ")";
            }
        }

        return "CAST(? AS " + typeName + ")";
    }

    @Override
    protected void registerTypes() {
        super.registerTypes();

        registerType(BytesType.INSTANCE);
        registerType(ZonedTimestampType.INSTANCE);
        registerType(ZonedTimeType.INSTANCE);
        registerType(DateType.INSTANCE);
        registerType(ConnectDateType.INSTANCE);
        registerType(ConnectTimeType.INSTANCE);
        registerType(ConnectTimestampType.INSTANCE);
        registerType(TimestampType.INSTANCE);
        registerType(NanoTimestampType.INSTANCE);
        registerType(MicroTimestampType.INSTANCE);
        registerType(TimeType.INSTANCE);
        registerType(NanoTimeType.INSTANCE);
        registerType(MicroTimeType.INSTANCE);
    }

    @Override
    public int getMaxVarcharLengthInKey() {
        // It would seem for Db2 for i, the maximum key size is 32768 bytes in total for all columns.
        // If other columns participate in the primary key, this reduces the size for a string-based column.
        // For simplicity, the connector will default to 32768, and ideally users for Db2 should create tables
        // manually that require more precision on column lengths within the primary key if the primary key
        // consists of a string-based column type.
        return 32768;
    }

    @Override
    public int getMaxNVarcharLengthInKey() {
        return 255;
    }

    @Override
    public String getAlterTablePrefix() {
        return "";
    }

    @Override
    public String getAlterTableSuffix() {
        return "";
    }

    @Override
    public String getAlterTableColumnPrefix() {
        return "ADD COLUMN";
    }

    @Override
    public String getAlterTableColumnDelimiter() {
        return " ";
    }

    @Override
    public String getTimestampPositiveInfinityValue() {
        return "9999-12-31T23:59:59+00:00";
    }

    @Override
    public String getTimestampNegativeInfinityValue() {
        return "0001-01-01T00:00:00+00:00";
    }

    /**
     * Generates the MERGE (upsert) statement for DB2i.
     * <p>
     * DB2i-specific requirements:
     * <ul>
     *   <li>Uses SELECT from sysibm.sysdummy1 instead of VALUES clause</li>
     *   <li>Parameter markers must be explicitly CAST to their types</li>
     *   <li>Target table uses alias TGT to avoid ambiguity in ON clause</li>
     *   <li>SET clause does not use table qualification (DB2i differs from DB2 LUW)</li>
     * </ul>
     *
     * @param table the target table descriptor
     * @param record the sink record containing data to merge
     * @return the complete MERGE SQL statement
     */
    @Override
    public String getUpsertStatement(TableDescriptor table, JdbcSinkRecord record) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("merge into ");
        builder.append(getQualifiedTableName(table.getId()));
        builder.append(" as TGT"); // Add target table alias
        builder.append(" using (select ");
        builder.appendLists(record.keyFieldNames(), record.nonKeyFieldNames(),
                (name) -> columnQueryBindingFromField(name, table, record) + " as " + columnNameFromField(name, record));
        builder.append(" from sysibm.sysdummy1");
        builder.append(") as DAT on ");
        // ON clause uses target table alias
        builder.appendList(" AND ", record.keyFieldNames(), (name) -> getOnClause(name, table, record));
        if (!record.nonKeyFieldNames().isEmpty()) {
            builder.append(" WHEN MATCHED THEN UPDATE SET ");
            // SET clause does NOT use qualified names in DB2i
            builder.appendList(", ", record.nonKeyFieldNames(), (name) -> getMergeDatClause(name, table, record));
        }

        builder.append(" WHEN NOT MATCHED THEN INSERT(");
        builder.appendLists(",", record.nonKeyFieldNames(), record.keyFieldNames(), (name) -> columnNameFromField(name, record));
        builder.append(") values (");
        builder.appendLists(",", record.nonKeyFieldNames(), record.keyFieldNames(), (name) -> "DAT." + columnNameFromField(name, record));
        builder.append(")");

        return builder.build();
    }

    private String getOnClause(String fieldName, TableDescriptor table, JdbcSinkRecord record) {
        final String columnName = columnNameFromField(fieldName, record);
        // ON clause uses target table alias TGT
        return "TGT." + columnName + "=DAT." + columnName;
    }

    private String getMergeDatClause(String fieldName, TableDescriptor table, JdbcSinkRecord record) {
        final String columnName = columnNameFromField(fieldName, record);
        // This is where DB2 for i differs from DB2 LUW
        // DB2 for i does not use fully qualified names in the SET portion of the MERGE command
        //
        // DB2 LUW version would also qualify here: return toIdentifier(table.getId()) + "." + columnName + "=DAT." + columnName;
        // DB2 for i version (SET clause only):
        return columnName + "=DAT." + columnName;
    }

    @Override
    protected void addColumnDefaultValue(FieldDescriptor field, StringBuilder columnSpec) {
        if (field.getSchema().isOptional()) {
            return;
        }
        super.addColumnDefaultValue(field, columnSpec);
    }

    @Override
    protected boolean isIdentifierUppercaseWhenNotQuoted() {
        return true;
    }

    @Override
    public String getFormattedTime(TemporalAccessor value) {
        return String.format("'%s'", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(value));
    }

    @Override
    public String getFormattedDateTime(TemporalAccessor value) {
        return String.format("'%s'", ISO_LOCAL_DATE_TIME_WITH_SPACE.format(value));
    }

    @Override
    public String getFormattedTimestamp(TemporalAccessor value) {
        return String.format("'%s'", ISO_LOCAL_DATE_TIME_WITH_SPACE.format(value));
    }

    @Override
    public String getFormattedTimestampWithTimeZone(String value) {
        final ZonedDateTime zonedDateTime = ZonedDateTime.parse(value, ZonedTimestamp.FORMATTER);
        return String.format("'%s'", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(zonedDateTime));
    }

    @Override
    protected String resolveColumnNameFromField(String fieldName) {
        String columnName = super.resolveColumnNameFromField(fieldName);
        if (!getConfig().isQuoteIdentifiers()) {
            // There are specific use cases where we explicitly quote the column name, even if the
            // quoted identifiers is not enabled, such as the Kafka primary key mode column names.
            // If they're quoted, we shouldn't uppercase the column name.
            if (!getIdentifierHelper().toIdentifier(columnName).isQuoted()) {
                // Db2 defaults to uppercase for identifiers
                columnName = columnName.toUpperCase();
            }
        }
        return columnName;
    }

    @Override
    public String getTruncateStatement(TableDescriptor table) {
        // to-do review if we can use TRUNCATE statement in DB2 for i
        //
        // For some reason the TRUNCATE statement doesn't work for DB2 even if it is supported from 9.7 https://www.ibm.com/support/pages/apar/JR37942
        // The problem verifies with Hibernate, plain JDBC works good.
        // Qlik uses the below approach https://community.qlik.com/t5/Qlik-Replicate/Using-Qlik-for-DB2-TRUNCATE-option/td-p/1989498
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("ALTER TABLE ");
        builder.append(getQualifiedTableName(table.getId()));
        builder.append(" ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE");
        return builder.build();
    }

}
