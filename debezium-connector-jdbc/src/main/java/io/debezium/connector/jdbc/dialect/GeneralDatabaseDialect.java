/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.Size;
import org.hibernate.engine.jdbc.env.spi.IdentifierHelper;
import org.hibernate.engine.jdbc.env.spi.NameQualifierSupport;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.exception.JDBCConnectionException;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkRecord;
import io.debezium.connector.jdbc.field.JdbcFieldDescriptor;
import io.debezium.connector.jdbc.naming.ColumnNamingStrategy;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.type.connect.AbstractConnectSchemaType;
import io.debezium.connector.jdbc.type.connect.ConnectBooleanType;
import io.debezium.connector.jdbc.type.connect.ConnectBytesType;
import io.debezium.connector.jdbc.type.connect.ConnectDateType;
import io.debezium.connector.jdbc.type.connect.ConnectDecimalType;
import io.debezium.connector.jdbc.type.connect.ConnectFloat32Type;
import io.debezium.connector.jdbc.type.connect.ConnectFloat64Type;
import io.debezium.connector.jdbc.type.connect.ConnectInt16Type;
import io.debezium.connector.jdbc.type.connect.ConnectInt32Type;
import io.debezium.connector.jdbc.type.connect.ConnectInt64Type;
import io.debezium.connector.jdbc.type.connect.ConnectInt8Type;
import io.debezium.connector.jdbc.type.connect.ConnectMapToConnectStringType;
import io.debezium.connector.jdbc.type.connect.ConnectStringType;
import io.debezium.connector.jdbc.type.connect.ConnectTimeType;
import io.debezium.connector.jdbc.type.connect.ConnectTimestampType;
import io.debezium.connector.jdbc.type.debezium.DateType;
import io.debezium.connector.jdbc.type.debezium.DebeziumZonedTimestampType;
import io.debezium.connector.jdbc.type.debezium.MicroTimeType;
import io.debezium.connector.jdbc.type.debezium.MicroTimestampType;
import io.debezium.connector.jdbc.type.debezium.NanoTimeType;
import io.debezium.connector.jdbc.type.debezium.NanoTimestampType;
import io.debezium.connector.jdbc.type.debezium.TimeType;
import io.debezium.connector.jdbc.type.debezium.TimestampType;
import io.debezium.connector.jdbc.type.debezium.VariableScaleDecimalType;
import io.debezium.connector.jdbc.type.debezium.ZonedTimeType;
import io.debezium.data.vector.DoubleVector;
import io.debezium.data.vector.FloatVector;
import io.debezium.data.vector.SparseDoubleVector;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.field.FieldDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.util.Strings;

/**
 * A generalized ANSI92 compliant {@link DatabaseDialect} implementation.
 *
 * @author Chris Cranford
 */
public class GeneralDatabaseDialect implements DatabaseDialect {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeneralDatabaseDialect.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

    private final JdbcSinkConnectorConfig connectorConfig;
    private final Dialect dialect;
    private final DdlTypeRegistry ddlTypeRegistry;
    private final IdentifierHelper identifierHelper;
    private final ColumnNamingStrategy columnNamingStrategy;
    private final Map<String, JdbcType> typeRegistry = new HashMap<>();
    private final boolean jdbcTimeZone;

    public GeneralDatabaseDialect(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
        this.connectorConfig = config;
        this.dialect = unwrapSessionFactory(sessionFactory).getJdbcServices().getDialect();
        this.ddlTypeRegistry = unwrapSessionFactory(sessionFactory).getTypeConfiguration().getDdlTypeRegistry();
        this.identifierHelper = unwrapSessionFactory(sessionFactory).getJdbcServices().getJdbcEnvironment().getIdentifierHelper();
        this.columnNamingStrategy = connectorConfig.getColumnNamingStrategy();

        final String jdbcTimeZone = config.getHibernateConfiguration().getProperty(AvailableSettings.JDBC_TIME_ZONE);
        this.jdbcTimeZone = !Strings.isNullOrEmpty(jdbcTimeZone);

        registerTypes();

        LOGGER.info("Database TimeZone: {}", getDatabaseTimeZone(sessionFactory));
    }

    @Override
    public CollectionId getCollectionId(String tableName) {
        final String[] parts = io.debezium.relational.TableId.parseParts(tableName);

        if (parts.length == 3) {
            // If the parse returns 3 elements, this will be used by default regardless of the
            // name qualifier support dialect configuration.
            return new CollectionId(parts[0], parts[1], parts[2]);
        }
        else if (parts.length == 2) {
            // If a name qualifier support configuration is available and it supports catalogs but
            // no schemas, then the value will be injected into the database/catalog part, else it
            // will be used as the schema bit.
            final NameQualifierSupport nameQualifierSupport = dialect.getNameQualifierSupport();
            if (nameQualifierSupport != null && nameQualifierSupport.supportsCatalogs()) {
                if (!nameQualifierSupport.supportsSchemas()) {
                    return new CollectionId(parts[0], null, parts[1]);
                }
            }
            return new CollectionId(parts[0], parts[1]);
        }
        else if (parts.length == 1) {
            return new CollectionId(parts[0]);
        }
        else {
            throw new DebeziumException("Failed to parse table name into TableId: " + tableName);
        }
    }

    @Override
    public boolean tableExists(Connection connection, CollectionId collectionId) throws SQLException {
        if (isIdentifierUppercaseWhenNotQuoted() && !getConfig().isQuoteIdentifiers()) {
            collectionId = collectionId.toUpperCase();
        }
        try (ResultSet rs = connection.getMetaData().getTables(collectionId.realm(), collectionId.namespace(), collectionId.name(), null)) {
            return rs.next();
        }
    }

    @Override
    public TableDescriptor readTable(Connection connection, CollectionId collectionId) throws SQLException {
        if (isIdentifierUppercaseWhenNotQuoted() && !getConfig().isQuoteIdentifiers()) {
            collectionId = collectionId.toUpperCase();
        }
        final TableDescriptor.Builder table = TableDescriptor.builder();

        final DatabaseMetaData metadata = connection.getMetaData();
        try (ResultSet rs = metadata.getTables(collectionId.realm(), collectionId.namespace(), collectionId.name(), null)) {
            if (rs.next()) {
                table.catalogName(rs.getString(1));
                table.schemaName(rs.getString(2));
                table.tableName(collectionId.name());

                final String tableType = rs.getString(4);
                table.type(Strings.isNullOrBlank(tableType) ? "TABLE" : tableType);
            }
            else {
                throw new IllegalStateException("Failed to find table: " + collectionId.toFullIdentiferString());
            }
        }

        final List<String> primaryKeyColumNames = new ArrayList<>();
        try (ResultSet rs = metadata.getPrimaryKeys(collectionId.realm(), collectionId.namespace(), collectionId.name())) {
            while (rs.next()) {
                final String columnName = rs.getString(4);
                primaryKeyColumNames.add(columnName);
                table.keyColumn(columnName);
            }
        }

        try (ResultSet rs = metadata.getColumns(collectionId.realm(), collectionId.namespace(), collectionId.name(), null)) {
            final int resultSizeColumnSize = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                final String columnName = rs.getString(4);
                final int jdbcType = rs.getInt(5);
                final String typeName = rs.getString(6);
                final int precision = rs.getInt(7);
                final int scale = rs.getInt(9);
                final int nullable = rs.getInt(11);

                String autoIncrement = "no";
                if (resultSizeColumnSize >= 23) {
                    // Not all drivers include all columns, so we're checking before reading
                    final String autoIncrementValue = rs.getString(23);
                    if (!Strings.isNullOrBlank(autoIncrementValue)) {
                        autoIncrement = autoIncrementValue;
                    }
                }

                final ColumnDescriptor column = ColumnDescriptor.builder()
                        .columnName(columnName)
                        .jdbcType(jdbcType)
                        .typeName(typeName)
                        .precision(precision)
                        .scale(scale)
                        .nullable(isColumnNullable(columnName, primaryKeyColumNames, nullable))
                        .autoIncrement("yes".equalsIgnoreCase(autoIncrement))
                        .primarykey(primaryKeyColumNames.contains(columnName))
                        .build();

                table.column(column);
            }
        }
        return table.build();
    }

    @Override
    public Set<String> resolveMissingFields(JdbcSinkRecord record, TableDescriptor table) {
        final Set<String> missingFields = new LinkedHashSet<>();
        for (FieldDescriptor field : record.jdbcFields().values()) {
            String columnName = resolveColumnName(field);
            if (!table.hasColumn(columnName)) {
                missingFields.add(field.getName());
            }
        }
        return missingFields;
    }

    protected String resolveColumnName(FieldDescriptor field) {
        String columnName = columnNamingStrategy.resolveColumnName(field.getColumnName());
        if (!getConfig().isQuoteIdentifiers()) {
            if (isIdentifierUppercaseWhenNotQuoted()) {
                final String columnIdentifier = toIdentifier(columnName);
                if (!(columnIdentifier.startsWith("\"") && columnIdentifier.endsWith("\""))) {
                    return columnName.toUpperCase();
                }
            }
            return columnName.toLowerCase();
        }
        return columnName;
    }

    @Override
    public String getCreateTableStatement(JdbcSinkRecord record, CollectionId collectionId) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("CREATE TABLE ");
        builder.append(getQualifiedTableName(collectionId));
        builder.append(" (");

        Map<String, FieldDescriptor> allFields = record.allFields();

        // First handle key columns
        builder.appendLists(", ", record.keyFieldNames(), record.nonKeyFieldNames(), (name) -> {
            final FieldDescriptor field = allFields.get(name);
            final String columnName = toIdentifier(resolveColumnName(field));

            Schema fieldSchema = field.getSchema();
            final String columnType = getSchemaType(fieldSchema).getTypeName(fieldSchema, field.isKey());

            final StringBuilder columnSpec = new StringBuilder();
            columnSpec.append(columnName).append(" ").append(columnType);
            addColumnDefaultValue(field, columnSpec);

            if (field.isKey()) {
                columnSpec.append(" NOT NULL");
            }
            else {
                columnSpec.append(fieldSchema.isOptional() ? " NULL" : " NOT NULL");
            }

            return columnSpec.toString();
        });

        if (!record.keyFieldNames().isEmpty()) {
            builder.append(", PRIMARY KEY(");
            builder.appendList(", ", record.keyFieldNames(), (name) -> {
                final FieldDescriptor field = allFields.get(name);
                return toIdentifier(columnNamingStrategy.resolveColumnName(field.getColumnName()));
            });
            builder.append(")");
        }

        builder.append(")");
        return builder.build();
    }

    @Override
    public String getAlterTablePrefix() {
        return "ADD (";
    }

    @Override
    public String getAlterTableSuffix() {
        return ")";
    }

    @Override
    public String getAlterTableColumnPrefix() {
        return "";
    }

    @Override
    public String getAlterTableColumnSuffix() {
        return "";
    }

    @Override
    public String getAlterTableColumnDelimiter() {
        return ", ";
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
            addColumnSpec.append(toIdentifier(columnNamingStrategy.resolveColumnName(field.getColumnName())));
            Schema fieldSchema = field.getSchema();
            addColumnSpec.append(" ").append(getSchemaType(fieldSchema).getTypeName(fieldSchema, field.isKey()));
            addColumnDefaultValue(field, addColumnSpec);

            addColumnSpec.append(field.getSchema().isOptional() ? " NULL" : " NOT NULL");
            addColumnSpec.append(getAlterTableColumnSuffix());
            return addColumnSpec.toString();
        });
        builder.append(getAlterTableSuffix());
        return builder.build();
    }

    @Override
    public String getInsertStatement(TableDescriptor table, JdbcSinkRecord record) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("INSERT INTO ");

        builder.append(getQualifiedTableName(table.getId()));
        builder.append(" (");

        builder.appendLists(", ", record.keyFieldNames(), record.nonKeyFieldNames(), (name) -> columnNameFromField(name, record));

        builder.append(") VALUES (");

        builder.appendLists(", ", record.keyFieldNames(), record.nonKeyFieldNames(), (name) -> columnQueryBindingFromField(name, table, record));

        builder.append(")");

        return builder.build();
    }

    @Override
    public String getUpsertStatement(TableDescriptor table, JdbcSinkRecord record) {
        throw new UnsupportedOperationException("Upsert configurations are not supported for this dialect");
    }

    @Override
    public String getUpdateStatement(TableDescriptor table, JdbcSinkRecord record) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("UPDATE ");
        builder.append(getQualifiedTableName(table.getId()));
        builder.append(" SET ");
        builder.appendList(", ", record.nonKeyFieldNames(), (name) -> columnNameEqualsBinding(name, table, record));

        if (!record.keyFieldNames().isEmpty()) {
            builder.append(" WHERE ");
            builder.appendList(" AND ", record.keyFieldNames(), (name) -> columnNameEqualsBinding(name, table, record));
        }

        return builder.build();
    }

    @Override
    public String getMergeIntoStatement(TableDescriptor table, JdbcSinkRecord record) {
        throw new UnsupportedOperationException("Merge into is not supported for this dialect");
    }

    @Override
    public String getDeleteStatement(TableDescriptor table, JdbcSinkRecord record) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("DELETE FROM ");
        builder.append(getQualifiedTableName(table.getId()));

        if (!record.keyFieldNames().isEmpty()) {
            builder.append(" WHERE ");
            builder.appendList(" AND ", record.keyFieldNames(), (name) -> columnNameEqualsBinding(name, table, record));
        }

        return builder.build();
    }

    @Override
    public String getTruncateStatement(TableDescriptor table) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("TRUNCATE TABLE ");
        builder.append(getQualifiedTableName(table.getId()));

        return builder.build();
    }

    @Override
    public String getQueryBindingWithValueCast(ColumnDescriptor column, Schema schema, JdbcType type) {
        return "?";
    }

    @Override
    public List<ValueBindDescriptor> bindValue(JdbcFieldDescriptor field, int startIndex, Object value) {
        LOGGER.trace("Bind field '{}' at position {} with type {}: {}", field.getName(), startIndex, getSchemaType(field.getSchema()).getClass().getName(), value);
        return field.bind(startIndex, value);
    }

    @Override
    public int getMaxVarcharLengthInKey() {
        return dialect.getMaxVarcharLength();
    }

    @Override
    public int getMaxNVarcharLengthInKey() {
        // Default to max non-nationalized sizes by default
        return getMaxVarcharLengthInKey();
    }

    @Override
    public int getMaxVarbinaryLength() {
        return dialect.getMaxVarbinaryLength();
    }

    @Override
    public boolean isTimeZoneSet() {
        return jdbcTimeZone;
    }

    @Override
    public boolean shouldBindTimeWithTimeZoneAsDatabaseTimeZone() {
        return false;
    }

    @Override
    public JdbcType getSchemaType(Schema schema) {
        if (!Objects.isNull(schema.name())) {
            final JdbcType type = typeRegistry.get(schema.name());
            if (!Objects.isNull(type)) {
                LOGGER.trace("Schema '{}' resolved by name from registry to type '{}'", schema.name(), type);
                return type;
            }
        }
        if (!Objects.isNull(schema.parameters())) {
            final String columnType = schema.parameters().get("__debezium.source.column.type");
            if (!Objects.isNull(columnType)) {
                final JdbcType type = typeRegistry.get(columnType);
                // We explicitly test whether the returned type is an AbstractConnectSchemaType because there
                // are use cases when column propagation is enabled and the source's column type may also map
                // directly to a raw Kafka schema type, i.e. INT8 from PostgreSQL. This prevents accidentally
                // resolving the dialect column type to a raw schema type.
                //
                // So in other words, if the column type is INT8 and the only registration is for the raw
                // schema type provided by Kafka Connect, this will not use that type and will fallback to
                // the lookup below based on the schema type's name, which in this case is also INT8.
                if (!Objects.isNull(type) && !(type instanceof AbstractConnectSchemaType)) {
                    LOGGER.trace("Schema '{}' resolved by name from registry to type '{}' using parameter '{}'",
                            schema, type, columnType);
                    return type;
                }
            }
        }

        final JdbcType type = typeRegistry.get(schema.type().name());
        if (!Objects.isNull(type)) {
            LOGGER.trace("Schema type '{}' resolved by name from registry to type '{}'", schema.type().name(), type);
            return type;
        }

        switch (schema.name()) {
            case SparseDoubleVector.LOGICAL_NAME, FloatVector.LOGICAL_NAME, DoubleVector.LOGICAL_NAME ->
                throw new ConnectException(
                        String.format(
                                "Dialect does not support schema type %s. Please use the VectorToJsonConverter transform in " +
                                        "your connector configuration to ingest data of this type.",
                                schema.name()));
            default -> throw new ConnectException(
                    String.format(
                            "Failed to resolve column type for schema: %s (%s)",
                            schema.type(),
                            schema.name()));
        }
    }

    @Override
    public DatabaseVersion getVersion() {
        return dialect.getVersion();
    }

    @Override
    public int getDefaultDecimalPrecision() {
        return dialect.getDefaultDecimalPrecision();
    }

    @Override
    public int getDefaultTimestampPrecision() {
        return dialect.getDefaultTimestampPrecision();
    }

    @Override
    public boolean isNegativeScaleAllowed() {
        return false;
    }

    @Override
    public String getJdbcTypeName(int jdbcType) {
        // To remain consistent with Debezium 2.x releases, the behavior with how column types were
        // resolved changed in Hibernate 6.3 to align more closely with JPA. This creates an issue
        // for us as we were relying on Hibernate for column type resolution, and now column types
        // are being resolved differently. This code aims to retain the Debezium 2.x resolution
        // functionality.
        return switch (jdbcType) {
            case Types.VARCHAR -> getJdbcTypeName(Types.LONGVARCHAR);
            case Types.NVARCHAR -> getJdbcTypeName(Types.LONGNVARCHAR);
            case Types.VARBINARY -> getJdbcTypeName(Types.LONGVARBINARY);
            default -> ddlTypeRegistry.getTypeName(jdbcType, dialect);
        };
    }

    @Override
    public String getJdbcTypeName(int jdbcType, Size size) {
        return ddlTypeRegistry.getTypeName(jdbcType, size);
    }

    @Override
    public String getTimestampPositiveInfinityValue() {
        return Timestamp.from(Instant.MAX).toString();
    }

    @Override
    public String getTimestampNegativeInfinityValue() {
        return Timestamp.from(Instant.MIN).toString();
    }

    @Override
    public String getByteArrayFormat() {
        return "x'%s'";
    }

    @Override
    public String getFormattedBoolean(boolean value) {
        // Map true to 1, false to 0 by default
        return value ? "1" : "0";
    }

    @Override
    public String getFormattedDate(TemporalAccessor value) {
        return String.format("'%s'", DATE_FORMATTER.format(value));
    }

    @Override
    public String getFormattedTime(TemporalAccessor value) {
        return String.format("'%s'", DateTimeFormatter.ISO_TIME.format(value));
    }

    @Override
    public String getFormattedTimeWithTimeZone(String value) {
        return String.format("'%s'", value);
    }

    @Override
    public String getFormattedDateTime(TemporalAccessor value) {
        return String.format("'%s'", DateTimeFormatter.ISO_DATE_TIME.format(value));
    }

    @Override
    public String getFormattedDateTimeWithNanos(TemporalAccessor value) {
        return getFormattedDateTime(value);
    }

    @Override
    public String getFormattedTimestamp(TemporalAccessor value) {
        return String.format("'%s'", DateTimeFormatter.ISO_ZONED_DATE_TIME.format(value));
    }

    @Override
    public String getFormattedTimestampWithTimeZone(String value) {
        return String.format("'%s'", value);
    }

    @Override
    public Set<Class<? extends Exception>> getCommunicationExceptions() {
        Set<Class<? extends Exception>> exceptions = new HashSet<>();
        if (connectorConfig.isConnectionRestartOnErrors()) {
            exceptions.add(JDBCConnectionException.class);
        }
        return exceptions;
    }

    protected String getJdbcTypeName(int jdbcType, int length) {
        return getJdbcTypeName(jdbcType, Size.length(length));
    }

    protected String getDatabaseTimeZone(SessionFactory sessionFactory) {
        final Optional<String> query = getDatabaseTimeZoneQuery();
        if (query.isPresent()) {
            try (StatelessSession session = sessionFactory.openStatelessSession()) {
                return session.doReturningWork((connection) -> {
                    try (Statement st = connection.createStatement()) {
                        try (ResultSet rs = st.executeQuery(query.get())) {
                            if (rs.next()) {
                                return getDatabaseTimeZoneQueryResult(rs);
                            }
                        }
                    }
                    return "N/A";
                });
            }
            catch (Exception e) {
                // ignored
            }
        }
        return "N/A";
    }

    protected Optional<String> getDatabaseTimeZoneQuery() {
        return Optional.empty();
    }

    protected String getDatabaseTimeZoneQueryResult(ResultSet rs) throws SQLException {
        return rs.getString(1);
    }

    protected void registerTypes() {
        // todo: Need to add support for these data types
        //
        // Oracle and PostgreSQL:
        // io.debezium.time.interval - partially supported with PostgreSQL
        // io.debezium.time.MicroDuration - partially supported with PostgreSQL
        // io.debezium.time.NAnoDuration - not supported; emitted by Cassandra
        //

        // Supported common Debezium data types
        registerType(DateType.INSTANCE);
        registerType(TimeType.INSTANCE);
        registerType(MicroTimeType.INSTANCE);
        registerType(TimestampType.INSTANCE);
        registerType(MicroTimestampType.INSTANCE);
        registerType(NanoTimeType.INSTANCE);
        registerType(NanoTimestampType.INSTANCE);
        registerType(ZonedTimeType.INSTANCE);
        registerType(DebeziumZonedTimestampType.INSTANCE);
        registerType(VariableScaleDecimalType.INSTANCE);

        // Supported connect data types
        registerType(ConnectBooleanType.INSTANCE);
        registerType(ConnectBytesType.INSTANCE);
        registerType(ConnectDateType.INSTANCE);
        registerType(ConnectDecimalType.INSTANCE);
        registerType(ConnectFloat32Type.INSTANCE);
        registerType(ConnectFloat64Type.INSTANCE);
        registerType(ConnectInt8Type.INSTANCE);
        registerType(ConnectInt16Type.INSTANCE);
        registerType(ConnectInt32Type.INSTANCE);
        registerType(ConnectInt64Type.INSTANCE);
        registerType(ConnectStringType.INSTANCE);
        registerType(ConnectTimestampType.INSTANCE);
        registerType(ConnectTimeType.INSTANCE);
        registerType(ConnectMapToConnectStringType.INSTANCE);
    }

    protected void registerType(JdbcType type) {
        type.configure(connectorConfig, this);
        for (String key : type.getRegistrationKeys()) {
            final JdbcType existing = typeRegistry.put(key, type);
            if (existing != null) {
                LOGGER.debug("JdbcType replaced [{}]: {} -> {}", key, existing.getClass().getName(), type.getClass().getName());
            }
            else {
                LOGGER.debug("JdbcType registered [{}]: {}", key, type.getClass().getName());
            }
        }
    }

    protected JdbcSinkConnectorConfig getConfig() {
        return connectorConfig;
    }

    protected DatabaseVersion getDatabaseVersion() {
        return dialect.getVersion();
    }

    protected IdentifierHelper getIdentifierHelper() {
        return identifierHelper;
    }

    protected void addColumnDefaultValue(FieldDescriptor field, StringBuilder columnSpec) {
        if (field.getSchema().defaultValue() != null) {
            final String defaultValue = getSchemaType(field.getSchema()).getDefaultValueBinding(field.getSchema(), field.getSchema().defaultValue());
            if (defaultValue != null) {
                columnSpec.append(" DEFAULT ").append(defaultValue);
            }
        }
    }

    protected String columnQueryBindingFromField(String fieldName, TableDescriptor table, JdbcSinkRecord record) {
        final FieldDescriptor field = record.allFields().get(fieldName);
        final String columnName = resolveColumnName(field);
        final ColumnDescriptor column = table.getColumnByName(columnName);

        final Object value;
        if (record.nonKeyFieldNames().contains(fieldName)) {
            value = getColumnValueFromValueField(fieldName, record);
        }
        else {
            value = getColumnValueFromKeyField(fieldName, record, columnName);
        }
        return record.jdbcFields().get(fieldName).getQueryBinding(column, value);
    }

    private Object getColumnValueFromKeyField(String fieldName, JdbcSinkRecord record, String columnName) {
        Object value;
        if (connectorConfig.getPrimaryKeyMode() == JdbcSinkConnectorConfig.PrimaryKeyMode.KAFKA) {
            value = getColumnValueForKafkaKeyMode(columnName, record);
        }
        else {
            final Struct source = record.filteredKey();
            value = source.get(fieldName);
        }
        return value;
    }

    private Object getColumnValueFromValueField(String fieldName, JdbcSinkRecord record) {
        return record.getPayload().get(fieldName);
    }

    private Object getColumnValueForKafkaKeyMode(String columnName, JdbcSinkRecord record) {
        return switch (columnName) {
            case "__connect_topic" -> record.topicName();
            case "__connect_partition" -> record.partition();
            case "__connect_offset" -> record.offset();
            default -> null;
        };
    }

    protected String columnNameFromField(String fieldName, JdbcSinkRecord record) {
        final FieldDescriptor field = record.allFields().get(fieldName);
        return toIdentifier(resolveColumnName(field));
    }

    protected String columnNameFromField(String fieldName, String prefix, JdbcSinkRecord record) {
        return prefix + columnNameFromField(fieldName, record);
    }

    protected String toIdentifier(String text) {
        final Identifier identifier = getIdentifierHelper().toIdentifier(text, getConfig().isQuoteIdentifiers());
        return identifier != null ? identifier.render(dialect) : text;
    }

    protected String toIdentifier(CollectionId collectionId) {
        final boolean quoted = getConfig().isQuoteIdentifiers();
        final Identifier catalog = getIdentifierHelper().toIdentifier(collectionId.realm(), quoted);
        final Identifier schema = getIdentifierHelper().toIdentifier(collectionId.namespace(), quoted);
        final Identifier table = getIdentifierHelper().toIdentifier(collectionId.name(), quoted);

        if (catalog != null && schema != null && table != null) {
            return String.format("%s.%s.%s", catalog.render(dialect), schema.render(dialect), table.render(dialect));
        }
        else if (schema != null && table != null) {
            return String.format("%s.%s", schema.render(dialect), table.render(dialect));
        }
        else if (table != null) {
            return table.render(dialect);
        }
        else {
            throw new IllegalStateException("Expected at least table identifier to be non-null");
        }
    }

    protected String resolveColumnNameFromField(String fieldName) {
        return columnNamingStrategy.resolveColumnName(fieldName);
    }

    protected boolean isIdentifierUppercaseWhenNotQuoted() {
        return false;
    }

    protected String getQualifiedTableName(CollectionId collectionId) {
        if (!Strings.isNullOrBlank(collectionId.namespace())) {
            return toIdentifier(collectionId.namespace()) + "." + toIdentifier(collectionId.name());
        }
        return toIdentifier(collectionId.name());
    }

    private String columnNameEqualsBinding(String fieldName, TableDescriptor table, JdbcSinkRecord record) {
        final JdbcFieldDescriptor field = record.jdbcFields().get(fieldName);
        final String columnName = resolveColumnName(field);
        final ColumnDescriptor column = table.getColumnByName(columnName);
        return toIdentifier(columnName) + "=" + field.getQueryBinding(column, record.getPayload());
    }

    private static boolean isColumnNullable(String columnName, Collection<String> primaryKeyColumnNames, int nullability) {
        if (primaryKeyColumnNames.contains(columnName)) {
            // Explicitly mark all primary keys are not nullable
            return false;
        }
        return nullability != DatabaseMetaData.columnNoNulls;
    }

    private static SessionFactoryImplementor unwrapSessionFactory(SessionFactory sessionFactory) {
        return sessionFactory.unwrap(SessionFactoryImplementor.class);
    }

}
