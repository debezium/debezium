/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.PessimisticLockException;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.Size;
import org.hibernate.exception.LockAcquisitionException;
import org.hibernate.exception.LockTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkRecord;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.GeneralDatabaseDialect;
import io.debezium.connector.jdbc.dialect.SqlStatementBuilder;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.field.FieldDescriptor;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Strings;

/**
 * A {@link DatabaseDialect} implementation for StarRocks.
 *
 * StarRocks speaks the MySQL wire protocol but diverges from MySQL in its SQL surface:
 * upserts are performed with plain {@code INSERT} statements against PRIMARY KEY tables,
 * the {@code PRIMARY KEY} and {@code DISTRIBUTED BY} clauses live outside the column list
 * in {@code CREATE TABLE}, {@code VARCHAR} lengths are measured in bytes rather than
 * characters, and several MySQL types (TIME, ENUM, SET, YEAR, the TEXT/BLOB families)
 * do not exist.
 */
public class StarRocksDatabaseDialect extends GeneralDatabaseDialect {

    private static final Logger LOGGER = LoggerFactory.getLogger(StarRocksDatabaseDialect.class);

    private static final List<String> NO_DEFAULT_VALUE_TYPES = List.of("json");

    private static final String NO_DEFAULT_VALUE_TYPE_PREFIX = "varbinary";

    /**
     * StarRocks VARCHAR/CHAR lengths are byte lengths. Source connectors propagate character
     * lengths, so lengths are multiplied by the maximum UTF-8 sequence size to avoid overflow
     * for multi-byte data.
     */
    private static final int UTF8_MAX_BYTES_PER_CHARACTER = 4;

    private static final int MAX_VARCHAR_LENGTH = 1_048_576;

    private static final int MAX_CHAR_LENGTH = 255;

    private static final DateTimeFormatter ISO_LOCAL_DATE_TIME_WITH_SPACE = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .toFormatter();

    public static class StarRocksDatabaseDialectProvider implements DatabaseDialectProvider {
        @Override
        public boolean supports(Dialect dialect) {
            return dialect instanceof StarRocksDialect;
        }

        @Override
        public Class<?> name() {
            return StarRocksDatabaseDialect.class;
        }

        @Override
        public DatabaseDialect instantiate(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
            LOGGER.info("StarRocks Dialect instantiated.");
            return new StarRocksDatabaseDialect(config, sessionFactory);
        }
    }

    private final boolean connectionTimeZoneSet;

    protected StarRocksDatabaseDialect(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
        super(config, sessionFactory);

        try (StatelessSession session = sessionFactory.openStatelessSession()) {
            this.connectionTimeZoneSet = session.doReturningWork((connection) -> connection.getMetaData().getURL().contains("connectionTimeZone="));
        }
    }

    @Override
    public boolean tableExists(Connection connection, CollectionId collectionId) throws SQLException {
        return super.tableExists(connection, qualifyWithCurrentDatabase(connection, collectionId));
    }

    @Override
    public TableDescriptor readTable(Connection connection, CollectionId collectionId) throws SQLException {
        return super.readTable(connection, qualifyWithCurrentDatabase(connection, collectionId));
    }

    /**
     * The StarRocks JDBC driver iterates over every database when JDBC metadata methods are
     * invoked without a database qualifier, and StarRocks raises an analyzing error rather than
     * returning an empty result for databases that do not contain the table, so metadata lookups
     * are qualified with the connection's current database.
     */
    private CollectionId qualifyWithCurrentDatabase(Connection connection, CollectionId collectionId) throws SQLException {
        if (collectionId.realm() == null && collectionId.namespace() == null) {
            final String database = connection.getCatalog();
            if (!Strings.isNullOrBlank(database)) {
                return new CollectionId(database, null, collectionId.name());
            }
        }
        return collectionId;
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
    protected void registerTypes() {
        super.registerTypes();

        registerType(BigIntUnsignedType.INSTANCE);
        registerType(BytesType.INSTANCE);
        registerType(EnumType.INSTANCE);
        registerType(SetType.INSTANCE);
        registerType(YearType.INSTANCE);
        registerType(JsonType.INSTANCE);
        registerType(MapToJsonType.INSTANCE);
        registerType(TimeType.INSTANCE);
        registerType(MicroTimeType.INSTANCE);
        registerType(NanoTimeType.INSTANCE);
        registerType(ConnectTimeType.INSTANCE);
        registerType(ZonedTimeType.INSTANCE);
        registerType(ZonedTimestampType.INSTANCE);
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
        // StarRocks CHAR/VARCHAR lengths are byte lengths while source connectors propagate
        // character lengths; scale the length so multi-byte (UTF-8) data cannot overflow.
        switch (jdbcType) {
            case Types.CHAR:
            case Types.NCHAR:
                if (size.getLength() != null && size.getLength() > 0) {
                    final long length = toByteLength(size.getLength());
                    if (length <= MAX_CHAR_LENGTH) {
                        return "char(" + length + ")";
                    }
                    return "varchar(" + length + ")";
                }
                break;
            case Types.VARCHAR:
            case Types.NVARCHAR:
                if (size.getLength() != null && size.getLength() > 0) {
                    return "varchar(" + toByteLength(size.getLength()) + ")";
                }
                break;
            case Types.VARBINARY:
                if (size.getLength() != null && size.getLength() > 0) {
                    return "varbinary(" + Math.min(size.getLength(), MAX_VARCHAR_LENGTH) + ")";
                }
                break;
            default:
                break;
        }
        return super.getJdbcTypeName(jdbcType, size);
    }

    private long toByteLength(long characterLength) {
        return Math.min(characterLength * UTF8_MAX_BYTES_PER_CHARACTER, MAX_VARCHAR_LENGTH);
    }

    @Override
    public int getMaxVarcharLengthInKey() {
        // StarRocks enforces a 128-byte limit on the encoded size of all primary key column
        // values of a PRIMARY KEY table at write time. The returned character length is scaled
        // by four for byte semantics, aligning the declared column limit with that bound.
        return 32;
    }

    @Override
    public String getCreateTableStatement(JdbcSinkRecord record, CollectionId collectionId) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("CREATE TABLE ");
        builder.append(getQualifiedTableName(collectionId));
        builder.append(" (");

        final Map<String, FieldDescriptor> allFields = record.allFields();

        // Key columns must be defined first for StarRocks PRIMARY KEY tables.
        builder.appendLists(", ", record.keyFieldNames(), record.nonKeyFieldNames(), (name) -> {
            final FieldDescriptor field = allFields.get(name);
            final String columnName = toIdentifier(resolveColumnName(field));

            final Schema fieldSchema = field.getSchema();
            final String columnType = getSchemaType(fieldSchema).getTypeName(fieldSchema, field.isKey());

            final StringBuilder columnSpec = new StringBuilder();
            columnSpec.append(columnName).append(" ").append(columnType);

            // StarRocks requires the NULL/NOT NULL constraint before the DEFAULT clause.
            if (field.isKey()) {
                columnSpec.append(" NOT NULL");
            }
            else {
                columnSpec.append(fieldSchema.isOptional() ? " NULL" : " NOT NULL");
            }
            addColumnDefaultValue(field, columnSpec);

            return columnSpec.toString();
        });

        builder.append(")");

        // Unlike MySQL, the PRIMARY KEY clause is defined outside the column list and creates a
        // StarRocks PRIMARY KEY table, which treats INSERT operations as upserts. Without key
        // fields a duplicate key table is created, which only supports appends.
        if (!record.keyFieldNames().isEmpty()) {
            builder.append(" PRIMARY KEY (");
            builder.appendList(", ", record.keyFieldNames(), (name) -> {
                final FieldDescriptor field = allFields.get(name);
                return toIdentifier(getConfig().getColumnNamingStrategy().resolveColumnName(field.getColumnName()));
            });
            builder.append(") DISTRIBUTED BY HASH (");
            builder.appendList(", ", record.keyFieldNames(), (name) -> {
                final FieldDescriptor field = allFields.get(name);
                return toIdentifier(getConfig().getColumnNamingStrategy().resolveColumnName(field.getColumnName()));
            });
            builder.append(")");
        }

        return builder.build();
    }

    @Override
    public String getAlterTableStatement(TableDescriptor table, JdbcSinkRecord record, Set<String> missingFields) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("ALTER TABLE ");
        builder.append(getQualifiedTableName(table.getId()));
        builder.append(" ");
        builder.append(getAlterTablePrefix());
        builder.appendList(getAlterTableColumnDelimiter(), missingFields, (name) -> {
            final FieldDescriptor field = record.allFields().get(name);
            final StringBuilder addColumnSpec = new StringBuilder();
            addColumnSpec.append(getAlterTableColumnPrefix());
            addColumnSpec.append(" ");
            addColumnSpec.append(toIdentifier(getConfig().getColumnNamingStrategy().resolveColumnName(field.getColumnName())));
            final Schema fieldSchema = field.getSchema();
            addColumnSpec.append(" ").append(getSchemaType(fieldSchema).getTypeName(fieldSchema, field.isKey()));

            // StarRocks requires the NULL/NOT NULL constraint before the DEFAULT clause.
            addColumnSpec.append(fieldSchema.isOptional() ? " NULL" : " NOT NULL");
            addColumnDefaultValue(field, addColumnSpec);

            addColumnSpec.append(getAlterTableColumnSuffix());
            return addColumnSpec.toString();
        });
        builder.append(getAlterTableSuffix());
        return builder.build();
    }

    @Override
    public String getUpsertStatement(TableDescriptor table, JdbcSinkRecord record) {
        // StarRocks does not support INSERT ... ON DUPLICATE KEY UPDATE; an INSERT that lists
        // all columns of a PRIMARY KEY table is executed as an upsert.
        return getInsertStatement(table, record);
    }

    @Override
    public String getAlterTablePrefix() {
        return "ADD COLUMN (";
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
        // StarRocks DATETIME supports 0000-01-01 through 9999-12-31; leave a day of headroom
        // for time zone conversions.
        return "9999-12-31T00:00:00+00:00";
    }

    @Override
    public String getTimestampNegativeInfinityValue() {
        return "0000-01-02T00:00:00+00:00";
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
        final Schema fieldSchema = field.getSchema();
        final String fieldType = getSchemaType(fieldSchema).getTypeName(fieldSchema, field.isKey());
        if (!Strings.isNullOrBlank(fieldType)) {
            final String type = fieldType.toLowerCase();
            if (NO_DEFAULT_VALUE_TYPES.contains(type) || type.startsWith(NO_DEFAULT_VALUE_TYPE_PREFIX)) {
                // StarRocks does not permit DEFAULT clauses for these types.
                return;
            }
        }
        super.addColumnDefaultValue(field, columnSpec);
    }
}
