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

import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MariaDBDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.SinkRecordDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.GeneralDatabaseDialect;
import io.debezium.connector.jdbc.dialect.SqlStatementBuilder;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Strings;

/**
 * A {@link DatabaseDialect} implementation for MariaDB.
 *
 */
public class MariaDbDatabaseDialect extends GeneralDatabaseDialect {

    private static final Logger LOGGER = LoggerFactory.getLogger(MariaDbDatabaseDialect.class);

    private static final List<String> NO_DEFAULT_VALUE_TYPES = Arrays.asList(
            "tinytext", "mediumtext", "longtext", "text", "tinyblob", "mediumblob", "longblob");

    private static final DateTimeFormatter ISO_LOCAL_DATE_TIME_WITH_SPACE = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .toFormatter();

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

    private final boolean connectionTimeZoneSet;

    private MariaDbDatabaseDialect(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
        super(config, sessionFactory);
        try (StatelessSession session = sessionFactory.openStatelessSession()) {
            this.connectionTimeZoneSet = session.doReturningWork(connection -> connection.getMetaData().getURL().contains("connectionTimeZone="));
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
    public String getAlterTablePrefix() {
        return "ADD COLUMN (";
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
