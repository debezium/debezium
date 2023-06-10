/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.oracle;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

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
    protected void registerTypes() {
        super.registerTypes();

        registerType(NumberType.INSTANCE);
        registerType(BytesType.INSTANCE);
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
                (name) -> columnQueryBindingFromField(name, record) + " " + columnNameFromField(name, record));
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
    public String getFormattedDate(ZonedDateTime value) {
        return String.format(TO_DATE, super.getFormattedDate(value));
    }

    @Override
    public String getFormattedTime(ZonedDateTime value) {
        return String.format(TO_TIMESTAMP_FF9, DateTimeFormatter.ISO_ZONED_DATE_TIME.format(value));
    }

    @Override
    public String getFormattedDateTime(ZonedDateTime value) {
        return String.format(TO_TIMESTAMP_FF6, DateTimeFormatter.ISO_ZONED_DATE_TIME.format(value));
    }

    @Override
    public String getFormattedTimestamp(ZonedDateTime value) {
        return String.format(TO_TIMESTAMP_FF6, super.getFormattedTimestamp(value));
    }

    @Override
    public String getFormattedTimestampWithTimeZone(String value) {
        return String.format(TO_TIMESTAMP_FF9_TZ, value);
    }

    private String getUpsertIncomingClause(String fieldName, TableDescriptor table, SinkRecordDescriptor record) {
        final String columnName = columnNameFromField(fieldName, record);
        return toIdentifier(table.getId()) + "." + columnName + "=INCOMING." + columnName;
    }
}
