/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.PessimisticLockException;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.engine.jdbc.Size;
import org.hibernate.exception.LockAcquisitionException;
import org.hibernate.exception.LockTimeoutException;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkRecord;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.GeneralDatabaseDialect;
import io.debezium.connector.jdbc.dialect.SqlStatementBuilder;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.field.FieldDescriptor;
import io.debezium.time.ZonedTimestamp;
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
    private final SessionFactory sessionFactory;

    protected MySqlDatabaseDialect(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
        super(config, sessionFactory);
        this.sessionFactory = sessionFactory;

        try (StatelessSession session = sessionFactory.openStatelessSession()) {
            this.connectionTimeZoneSet = session.doReturningWork((connection) -> connection.getMetaData().getURL().contains("connectionTimeZone="));
        }
    }

    /**
     * Renders the given instant as the wall clock of the session time zone of the target database.
     * <p>
     * A {@code TIMESTAMP} literal carries no offset, so the server resolves it against the session
     * time zone of the connection that executes the statement, and the bound-value path ends in the
     * same place, because the driver converts a bound value into the session time zone before it
     * sends it. A column default must therefore be the session-zone wall clock of the instant it is
     * meant to store, and not its UTC wall clock.
     * <p>
     * The source zone is the numeric offset {@code '+00:00'} and not the name {@code 'UTC'}, because
     * a named zone is resolved through the {@code mysql.time_zone_name} table and is unknown on a
     * server where that table is empty, where {@code CONVERT_TZ} returns {@code null}.
     *
     * @param value the instant to render
     * @return the wall clock of the instant in the session time zone
     */
    private String toSessionZoneLiteral(ZonedDateTime value) {
        final LocalDateTime utc = LocalDateTime.ofInstant(value.toInstant(), ZoneOffset.UTC);
        try (StatelessSession session = sessionFactory.openStatelessSession()) {
            return session.doReturningWork((connection) -> {
                try (PreparedStatement statement = connection.prepareStatement(
                        "SELECT REPLACE(CONVERT_TZ(?, '+00:00', @@session.time_zone), ' ', 'T')")) {
                    statement.setString(1, ISO_LOCAL_DATE_TIME_WITH_SPACE.format(utc));
                    try (ResultSet converted = statement.executeQuery()) {
                        if (converted.next() && converted.getString(1) != null) {
                            return converted.getString(1);
                        }
                    }
                }
                throw new ConnectException("Failed to resolve the session time zone offset from the target database");
            });
        }
        catch (Exception e) {
            throw new ConnectException("Failed to resolve the session time zone offset from the target database", e);
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
        registerType(BigIntUnsignedType.INSTANCE);
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
        registerType(StructToJsonType.INSTANCE);
        registerType(GeometryType.INSTANCE);
        registerType(PointType.INSTANCE);
        registerType(ZonedTimestampType.INSTANCE);
        registerType(ZonedTimeType.INSTANCE);
        registerType(StructuredDateType.INSTANCE);
        registerType(StructuredTimestampType.INSTANCE);
        registerType(StructuredZonedTimestampType.INSTANCE);
        registerType(StructuredDurationType.INSTANCE);

        registerType(FloatVectorType.INSTANCE);
        registerType(DoubleVectorType.INSTANCE);
    }

    @Override
    public JdbcType getSchemaType(Schema schema) {
        if (BigIntUnsignedType.INSTANCE.matchesSourceColumnType(schema)) {
            return BigIntUnsignedType.INSTANCE;
        }
        return super.getSchemaType(schema);
    }

    @Override
    public String getJdbcTypeName(int jdbcType, Size size) {
        // Hibernate 7.1 began to map CHAR(n) to VARCHAR(n) instead - this restores the logic
        switch (jdbcType) {
            case Types.CHAR:
            case Types.NCHAR:
                if (size.getLength() != null && size.getLength() > 0) {
                    return "char(" + size.getLength() + ")";
                }
        }
        return super.getJdbcTypeName(jdbcType, size);
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
        return String.format("'%s'", toSessionZoneLiteral(zonedDateTime));
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
    public String getUpsertStatement(TableDescriptor table, JdbcSinkRecord record) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("INSERT INTO ");
        builder.append(getQualifiedTableName(table.getId()));
        builder.append(" (");
        builder.appendLists(", ", record.keyFieldNames(), record.nonKeyFieldNames(), (name) -> columnNameFromField(name, record));
        builder.append(") VALUES (");
        builder.appendLists(", ", record.keyFieldNames(), record.nonKeyFieldNames(), (name) -> columnQueryBindingFromField(name, table, record));
        builder.append(") ");

        final Set<String> updateColumnNames = record.nonKeyFieldNames().isEmpty()
                ? record.keyFieldNames()
                : record.nonKeyFieldNames();

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
        Set<Class<? extends Exception>> exceptions = super.getCommunicationExceptions();
        exceptions.addAll(
                Set.of(LockTimeoutException.class,
                        LockAcquisitionException.class,
                        PessimisticLockException.class));
        return Collections.unmodifiableSet(exceptions);
    }

    @Override
    protected void addColumnDefaultValue(FieldDescriptor field, StringBuilder columnSpec) {
        Schema fieldSchema = field.getSchema();
        final String fieldType = getSchemaType(fieldSchema).getTypeName(fieldSchema, field.isKey());
        if (!Strings.isNullOrBlank(fieldType)) {
            if (NO_DEFAULT_VALUE_TYPES.contains(fieldType.toLowerCase())) {
                return;
            }
        }
        super.addColumnDefaultValue(field, columnSpec);
    }
}
