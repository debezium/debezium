/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.sqlserver;

import java.util.Optional;

import org.hibernate.SessionFactory;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.SQLServerDialect;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.SinkRecordDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.GeneralDatabaseDialect;
import io.debezium.connector.jdbc.dialect.SqlStatementBuilder;
import io.debezium.connector.jdbc.relational.TableDescriptor;

/**
 * A {@link DatabaseDialect} implementation for SQL Server.
 *
 * @author Chris Cranford
 */
public class SqlServerDatabaseDialect extends GeneralDatabaseDialect {

    public static class SqlServerDatabaseDialectProvider implements DatabaseDialectProvider {
        @Override
        public boolean supports(Dialect dialect) {
            return dialect instanceof SQLServerDialect;
        }

        @Override
        public Class<?> name() {
            return SqlServerDatabaseDialect.class;
        }

        @Override
        public DatabaseDialect instantiate(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
            return new SqlServerDatabaseDialect(config, sessionFactory);
        }
    }

    private SqlServerDatabaseDialect(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
        super(config, sessionFactory);
    }

    @Override
    protected Optional<String> getDatabaseTimeZoneQuery() {
        return Optional.of("SELECT CURRENT_TIMEZONE()");
    }

    @Override
    protected void registerTypes() {
        super.registerTypes();

        registerType(BitType.INSTANCE);
        registerType(XmlType.INSTANCE);
    }

    @Override
    public String getTimeQueryBinding() {
        return "cast(? as time(7))";
    }

    @Override
    public int getMaxVarcharLengthInKey() {
        return 900;
    }

    @Override
    public int getMaxTimePrecision() {
        return 7;
    }

    @Override
    public int getMaxTimestampPrecision() {
        return 7;
    }

    @Override
    public String getUpsertStatement(TableDescriptor table, SinkRecordDescriptor record) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("MERGE INTO ");
        builder.append(getQualifiedTableName(table.getId()));
        builder.append(" WITH (HOLDLOCK) AS TARGET USING (SELECT ");
        builder.appendLists(", ", record.getKeyFieldNames(), record.getNonKeyFieldNames(),
                (name) -> columnNameFromField(name, columnQueryBindingFromField(name, record) + " AS ", record));
        builder.append(") AS INCOMING ON (");
        builder.appendList(" AND ", record.getKeyFieldNames(), (name) -> {
            final String columnName = columnNameFromField(name, record);
            return "TARGET." + columnName + "=INCOMING." + columnName;
        });
        builder.append(")");

        if (!record.getNonKeyFieldNames().isEmpty()) {
            builder.append(" WHEN MATCHED THEN UPDATE SET ");
            builder.appendList(",", record.getNonKeyFieldNames(), (name) -> {
                final String columnName = columnNameFromField(name, record);
                return columnName + "=INCOMING." + columnName;
            });
        }

        builder.append(" WHEN NOT MATCHED THEN INSERT (");
        builder.appendLists(", ", record.getNonKeyFieldNames(), record.getKeyFieldNames(), (name) -> columnNameFromField(name, record));
        builder.append(") VALUES (");
        builder.appendLists(",", record.getNonKeyFieldNames(), record.getKeyFieldNames(), (name) -> columnNameFromField(name, "INCOMING.", record));
        builder.append(")");
        builder.append(";"); // SQL server requires this to be terminated this way.

        return builder.build();
    }

    @Override
    public String getByteArrayFormat() {
        return "CONVERT(VARBINARY, '0x%s')";
    }

}
