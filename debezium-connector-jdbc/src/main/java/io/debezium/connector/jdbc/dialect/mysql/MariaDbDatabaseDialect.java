/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.util.List;

import org.hibernate.SessionFactory;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MariaDBDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.SinkRecordDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.SqlStatementBuilder;
import io.debezium.connector.jdbc.relational.TableDescriptor;

/**
 * A {@link DatabaseDialect} implementation for MariaDB.
 *
 */
public class MariaDbDatabaseDialect extends MySqlDatabaseDialect {

    private static final Logger LOGGER = LoggerFactory.getLogger(MariaDbDatabaseDialect.class);

    public static class MariaDbDatabaseDialectProvider implements DatabaseDialectProvider {
        @Override
        public boolean supports(Dialect dialect) {
            return dialect instanceof MariaDBDialect;
        }

        @Override
        public Class<?> name() {
            return MariaDbDatabaseDialect.class;
        }

        @Override
        public DatabaseDialect instantiate(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
            LOGGER.info("MariaDB Dialect instantiated.");
            return new MariaDbDatabaseDialect(config, sessionFactory);
        }
    }

    private MariaDbDatabaseDialect(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
        super(config, sessionFactory);
    }

    /*
     * The use of VALUES() to refer to the new row and columns is deprecated in recent versions of MySQL,
     * but not followed by MariaDB yet.
     */
    @Override
    public String getUpsertStatement(TableDescriptor table, SinkRecordDescriptor record) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("INSERT INTO ");
        builder.append(getQualifiedTableName(table.getId()));
        builder.append(" (");
        builder.appendLists(", ", record.getKeyFieldNames(), record.getNonKeyFieldNames(), name -> columnNameFromField(name, record));
        builder.append(") VALUES (");
        builder.appendLists(", ", record.getKeyFieldNames(), record.getNonKeyFieldNames(), name -> columnQueryBindingFromField(name, table, record));
        builder.append(") ");

        final List<String> updateColumnNames = record.getNonKeyFieldNames().isEmpty()
                ? record.getKeyFieldNames()
                : record.getNonKeyFieldNames();

        builder.append("ON DUPLICATE KEY UPDATE ");
        builder.appendList(",", updateColumnNames, name -> {
            final String columnName = columnNameFromField(name, record);
            return columnName + "=VALUES(" + columnName + ")";
        });

        return builder.build();
    }

}
