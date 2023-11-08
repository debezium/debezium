/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Optional;

import org.hibernate.SessionFactory;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.OracleDialect;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.SinkRecordDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.GeneralDatabaseDialect;
import io.debezium.connector.jdbc.dialect.SqlStatementBuilder;
import io.debezium.connector.jdbc.relational.TableDescriptor;

/**
 * A {@link DatabaseDialect} implementation for Oracle.
 *
 * @author Chris Cranford
 */
public class OracleDatabaseDialect extends GeneralDatabaseDialect {

    private static final String TO_DATE = "TO_DATE(%s, 'YYYY-MM-DD')";
    private static final String TO_TIMESTAMP_FF9 = "TO_TIMESTAMP('%s', 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9 TZH:TZM')";
    private static final String TO_TIMESTAMP_FF6 = "TO_TIMESTAMP('%s', 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6 TZH:TZM')";
    private static final String TO_TIMESTAMP_FF9_TZ = "TO_TIMESTAMP_TZ('%s', 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9 TZH:TZM')";

    public static class OracleDatabaseDialectProvider implements DatabaseDialectProvider {
        @Override
        public boolean supports(Dialect dialect) {
            return dialect instanceof OracleDialect;
        }

        @Override
        public Class<?> name() {
            return OracleDatabaseDialect.class;
        }

        @Override
        public DatabaseDialect instantiate(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
            return new OracleDatabaseDialect(config, sessionFactory);
        }
    }

    private OracleDatabaseDialect(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
        super(config, sessionFactory);
    }

    @Override
    protected Optional<String> getDatabaseTimeZoneQuery() {
        return Optional.of("SELECT DBTIMEZONE, SESSIONTIMEZONE FROM DUAL");
    }

    @Override
    protected String getDatabaseTimeZoneQueryResult(ResultSet rs) throws SQLException {
        return rs.getString(1) + " (database), " + rs.getString(2) + " (session)";
    }

    @Override
    protected void registerTypes() {
        super.registerTypes();

        registerType(NumberType.INSTANCE);
        registerType(BytesType.INSTANCE);
        registerType(ZonedTimestampType.INSTANCE);
        registerType(ZonedTimeType.INSTANCE);
    }

    @Override
    public int getMaxVarcharLengthInKey() {
        return 4000;
    }

    @Override
    public int getMaxNVarcharLengthInKey() {
        return 2000;
    }

    @Override
    public boolean isNegativeScaleAllowed() {
        return true;
    }

    @Override
    public String getUpsertStatement(TableDescriptor table, SinkRecordDescriptor record) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("MERGE INTO ");
        builder.append(getQualifiedTableName(table.getId()));
        builder.append(" USING (SELECT ");
        builder.appendLists(", ", record.getKeyFieldNames(), record.getNonKeyFieldNames(),
                (name) -> columnQueryBindingFromField(name, table, record) + " " + columnNameFromField(name, record));
        builder.append(" FROM dual) ").append("INCOMING ON (");
        builder.appendList(" AND ", record.getKeyFieldNames(), (name) -> getUpsertIncomingClause(name, table, record));
        builder.append(")");
        if (!record.getNonKeyFieldNames().isEmpty()) {
            builder.append(" WHEN MATCHED THEN UPDATE SET ");
            builder.appendList(",", record.getNonKeyFieldNames(), (name) -> getUpsertIncomingClause(name, table, record));
        }
        builder.append(" WHEN NOT MATCHED THEN INSERT (");
        builder.appendLists(",", record.getNonKeyFieldNames(), record.getKeyFieldNames(), (name) -> columnNameFromField(name, record));
        builder.append(") VALUES (");
        builder.appendLists(",", record.getNonKeyFieldNames(), record.getKeyFieldNames(), (name) -> columnNameFromField(name, "INCOMING.", record));
        builder.append(")");
        return builder.build();
    }

    @Override
    protected boolean isIdentifierUppercaseWhenNotQuoted() {
        return true;
    }

    @Override
    public String getFormattedDate(TemporalAccessor value) {
        return String.format(TO_DATE, super.getFormattedDate(value));
    }

    @Override
    public String getFormattedTime(TemporalAccessor value) {
        // Oracle maps AbstractTimeType(s) as DATE columns
        // The value parsing may provide a LocalTime object, cast it to LocalDateTime based on EPOCH
        if (value instanceof LocalTime) {
            value = ((LocalTime) value).atDate(LocalDate.EPOCH);
        }
        return String.format(TO_TIMESTAMP_FF9, DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(value));
    }

    @Override
    public String getFormattedDateTime(TemporalAccessor value) {
        return String.format(TO_TIMESTAMP_FF6, DateTimeFormatter.ISO_ZONED_DATE_TIME.format(value));
    }

    @Override
    public String getFormattedTimestamp(TemporalAccessor value) {
        return String.format(TO_TIMESTAMP_FF6, DateTimeFormatter.ISO_ZONED_DATE_TIME.format(value));
    }

    @Override
    public String getFormattedTimestampWithTimeZone(String value) {
        return String.format(TO_TIMESTAMP_FF9_TZ, value);
    }

    @Override
    protected String resolveColumnNameFromField(String fieldName) {
        String columnName = super.resolveColumnNameFromField(fieldName);
        if (!getConfig().isQuoteIdentifiers()) {
            // There are specific use cases where we explicitly quote the column name, even if the
            // quoted identifiers is not enabled, such as the Kafka primary key mode column names.
            // If they're quoted, we shouldn't uppercase the column name.
            if (!getIdentifierHelper().toIdentifier(columnName).isQuoted()) {
                // Oracle defaults to uppercase for identifiers
                columnName = columnName.toUpperCase();
            }
        }
        return columnName;
    }

    private String getUpsertIncomingClause(String fieldName, TableDescriptor table, SinkRecordDescriptor record) {
        final String columnName = columnNameFromField(fieldName, record);
        return toIdentifier(table.getId()) + "." + columnName + "=INCOMING." + columnName;
    }
}
