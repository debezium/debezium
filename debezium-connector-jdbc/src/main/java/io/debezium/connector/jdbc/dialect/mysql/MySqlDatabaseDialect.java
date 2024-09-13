/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.hibernate.PessimisticLockException;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.exception.LockAcquisitionException;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.SinkRecordDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.GeneralDatabaseDialect;
import io.debezium.connector.jdbc.dialect.SqlStatementBuilder;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Collect;
import io.debezium.util.Strings;

/**
 * A {@link DatabaseDialect} implementation for MySQL.
 *
 * @author Chris Cranford
 */
public class MySqlDatabaseDialect extends GeneralDatabaseDialect {

    private static final List<String> NO_DEFAULT_VALUE_TYPES = Arrays.asList(
            "tinytext", "mediumtext", "longtext", "text", "tinyblob", "mediumblob", "longblob");

    private static final DateTimeFormatter ISO_LOCAL_DATE_TIME_WITH_SPACE = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .toFormatter();

    public static class MySqlDatabaseDialectProvider implements DatabaseDialectProvider {
        @Override
        public boolean supports(Dialect dialect) {
            return dialect instanceof MySQLDialect;
        }

        @Override
        public Class<?> name() {
            return MySqlDatabaseDialect.class;
        }

        @Override
        public DatabaseDialect instantiate(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
            return new MySqlDatabaseDialect(config, sessionFactory);
        }
    }

    private final boolean connectionTimeZoneSet;

    protected MySqlDatabaseDialect(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
        super(config, sessionFactory);

        try (StatelessSession session = sessionFactory.openStatelessSession()) {
            this.connectionTimeZoneSet = session.doReturningWork((connection) -> connection.getMetaData().getURL().contains("connectionTimeZone="));
        }
    }

    @Override
    protected Optional<String> getDatabaseTimeZoneQuery() {
        return Optional.of("SELECT @@global.time_zone, @@session.time_zone");
    }

    @Override
    protected String getDatabaseTimeZoneQueryResult(ResultSet rs) throws SQLException {
        return rs.getString(1) + " (global), " + rs.getString(2) + " (system)";
    }

    @Override
    public boolean isTimeZoneSet() {
        return connectionTimeZoneSet || super.isTimeZoneSet();
    }

    @Override
    public boolean shouldBindTimeWithTimeZoneAsDatabaseTimeZone() {
        return true;
    }

    @Override
    protected void registerTypes() {
        super.registerTypes();

        registerType(BooleanType.INSTANCE);
        registerType(BitType.INSTANCE);
        registerType(BytesType.INSTANCE);
        registerType(EnumType.INSTANCE);
        registerType(SetType.INSTANCE);
        registerType(MediumIntType.INSTANCE);
        registerType(IntegerType.INSTANCE);
        registerType(TinyIntType.INSTANCE);
        registerType(YearType.INSTANCE);
        registerType(JsonType.INSTANCE);
        registerType(MapToJsonType.INSTANCE);
        registerType(GeometryType.INSTANCE);
        registerType(PointType.INSTANCE);
        registerType(ZonedTimestampType.INSTANCE);
        registerType(ZonedTimeType.INSTANCE);
    }

    @Override
    public int getMaxVarcharLengthInKey() {
        return 255;
    }

    @Override
    public String getFormattedTime(TemporalAccessor value) {
        return String.format("'%s'", DateTimeFormatter.ISO_LOCAL_TIME.format(value));
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
    public String getTimestampPositiveInfinityValue() {
        return "2038-01-19T03:14:07+00:00";
    }

    @Override
    public String getTimestampNegativeInfinityValue() {
        return "1970-01-01T00:00:01+00:00";
    }

    @Override
    public String getAlterTablePrefix() {
        return "ADD COLUMN (";
    }

    @Override
    public String getUpsertStatement(TableDescriptor table, SinkRecordDescriptor record) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("INSERT INTO ");
        builder.append(getQualifiedTableName(table.getId()));
        builder.append(" (");
        builder.appendLists(", ", record.getKeyFieldNames(), record.getNonKeyFieldNames(), (name) -> columnNameFromField(name, record));
        builder.append(") VALUES (");
        builder.appendLists(", ", record.getKeyFieldNames(), record.getNonKeyFieldNames(), (name) -> columnQueryBindingFromField(name, table, record));
        builder.append(") ");

        final List<String> updateColumnNames = record.getNonKeyFieldNames().isEmpty()
                ? record.getKeyFieldNames()
                : record.getNonKeyFieldNames();

        if (getDatabaseVersion().isSameOrAfter(8, 0, 20)) {
            // MySQL 8.0.20 deprecated the use of "VALUES()" in exchange for table aliases
            builder.append("AS new ON DUPLICATE KEY UPDATE ");
            builder.appendList(",", updateColumnNames, (name) -> {
                final String columnName = columnNameFromField(name, record);
                return columnName + "=new." + columnName;
            });
        }
        else {
            builder.append("ON DUPLICATE KEY UPDATE ");
            builder.appendList(",", updateColumnNames, (name) -> {
                final String columnName = columnNameFromField(name, record);
                return columnName + "=VALUES(" + columnName + ")";
            });
        }

        return builder.build();
    }

    @Override
    public Set<Class<? extends Exception>> getCommunicationExceptions() {
        return Collect.unmodifiableSet(LockAcquisitionException.class, PessimisticLockException.class);
    }

    @Override
    protected void addColumnDefaultValue(SinkRecordDescriptor.FieldDescriptor field, StringBuilder columnSpec) {
        final String fieldType = field.getTypeName();
        if (!Strings.isNullOrBlank(fieldType)) {
            if (NO_DEFAULT_VALUE_TYPES.contains(fieldType.toLowerCase())) {
                return;
            }
        }
        super.addColumnDefaultValue(field, columnSpec);
    }
}
