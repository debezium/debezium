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
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.SessionFactory;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.Size;
import org.hibernate.engine.jdbc.env.spi.IdentifierHelper;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.query.NativeQuery;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.SinkRecordDescriptor;
import io.debezium.connector.jdbc.SinkRecordDescriptor.FieldDescriptor;
import io.debezium.connector.jdbc.naming.ColumnNamingStrategy;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.connector.jdbc.relational.TableId;
import io.debezium.connector.jdbc.type.Type;
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
import io.debezium.connector.jdbc.type.connect.ConnectStringType;
import io.debezium.connector.jdbc.type.connect.ConnectTimeType;
import io.debezium.connector.jdbc.type.connect.ConnectTimestampType;
import io.debezium.connector.jdbc.type.debezium.DateType;
import io.debezium.connector.jdbc.type.debezium.MicroTimeType;
import io.debezium.connector.jdbc.type.debezium.MicroTimestampType;
import io.debezium.connector.jdbc.type.debezium.NanoTimeType;
import io.debezium.connector.jdbc.type.debezium.NanoTimestampType;
import io.debezium.connector.jdbc.type.debezium.TimeType;
import io.debezium.connector.jdbc.type.debezium.TimestampType;
import io.debezium.connector.jdbc.type.debezium.VariableScaleDecimalType;
import io.debezium.connector.jdbc.type.debezium.ZonedTimeType;
import io.debezium.connector.jdbc.type.debezium.ZonedTimestampType;
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
    private final Map<String, Type> typeRegistry = new HashMap<>();

    public GeneralDatabaseDialect(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
        this.connectorConfig = config;
        this.dialect = unwrapSessionFactory(sessionFactory).getJdbcServices().getDialect();
        this.ddlTypeRegistry = unwrapSessionFactory(sessionFactory).getTypeConfiguration().getDdlTypeRegistry();
        this.identifierHelper = unwrapSessionFactory(sessionFactory).getJdbcServices().getJdbcEnvironment().getIdentifierHelper();
        this.columnNamingStrategy = connectorConfig.getColumnNamingStrategy();

        registerTypes();
    }

    @Override
    public boolean tableExists(Connection connection, String tableName) throws SQLException {
        if (isIdentifierUppercaseWhenNotQuoted() && !getConfig().isQuoteIdentifiers()) {
            tableName = Strings.isNullOrBlank(tableName) ? tableName : tableName.toUpperCase();
        }
        try (ResultSet rs = connection.getMetaData().getTables(null, null, tableName, null)) {
            return rs.next();
        }
    }

    @Override
    public TableDescriptor readTable(Connection connection, String tableName) throws SQLException {
        if (isIdentifierUppercaseWhenNotQuoted() && !getConfig().isQuoteIdentifiers()) {
            tableName = Strings.isNullOrBlank(tableName) ? tableName : tableName.toUpperCase();
        }
        final TableDescriptor.Builder table = TableDescriptor.builder();
        try (ResultSet rs = connection.getMetaData().getTables(null, null, tableName, null)) {
            if (rs.next()) {
                table.catalogName(rs.getString(1));
                table.schemaName(rs.getString(2));
                table.tableName(tableName);

                final String tableType = rs.getString(4);
                table.type(Strings.isNullOrBlank(tableType) ? "TABLE" : tableType);
            }
            else {
                throw new IllegalStateException("Failed to find table: " + tableName);
            }
        }

        final List<String> primaryKeyColumNames = new ArrayList<>();
        try (ResultSet rs = connection.getMetaData().getPrimaryKeys(null, null, tableName)) {
            while (rs.next()) {
                final String columnName = rs.getString(4);
                primaryKeyColumNames.add(columnName);
                table.keyColumn(columnName);
            }
        }

        try (ResultSet rs = connection.getMetaData().getColumns(null, null, tableName, null)) {
            final int resultSizeColumnSize = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                final String catalogName = rs.getString(1);
                final String schemaName = rs.getString(2);
                final String columnTableName = rs.getString(3);
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
    public Set<String> resolveMissingFields(SinkRecordDescriptor record, TableDescriptor table) {
        final Set<String> missingFields = new HashSet<>();
        for (FieldDescriptor field : record.getFields().values()) {
            String columnName = columnNamingStrategy.resolveColumnName(field.getName());
            if (isIdentifierUppercaseWhenNotQuoted() && !getConfig().isQuoteIdentifiers()) {
                final String columnIdentifier = toIdentifier(columnName);
                if (!(columnIdentifier.startsWith("\"") && columnIdentifier.endsWith("\""))) {
                    columnName = columnName.toUpperCase();
                }
            }
            if (!table.hasColumn(columnName)) {
                missingFields.add(field.getName());
            }
        }
        return missingFields;
    }

    @Override
    public String getCreateTableStatement(SinkRecordDescriptor record, String tableName) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("CREATE TABLE ");
        builder.append(toIdentifier(tableName));
        builder.append(" (");

        // First handle key columns
        builder.appendLists(", ", record.getKeyFieldNames(), record.getNonKeyFieldNames(), (name) -> {
            final FieldDescriptor field = record.getFields().get(name);
            final String columnName = toIdentifier(columnNamingStrategy.resolveColumnName(name));

            final String columnType = field.getTypeName();

            final StringBuilder columnSpec = new StringBuilder();
            columnSpec.append(columnName).append(" ").append(columnType);
            addColumnDefaultValue(field, columnSpec);

            if (field.isKey()) {
                columnSpec.append(" NOT NULL");
            }
            else {
                columnSpec.append(field.getSchema().isOptional() ? " NULL" : " NOT NULL");
            }

            return columnSpec.toString();
        });

        if (!record.getKeyFieldNames().isEmpty()) {
            builder.append(", PRIMARY KEY(");
            builder.appendList(", ", record.getKeyFieldNames(), (name) -> {
                final FieldDescriptor field = record.getFields().get(name);
                return toIdentifier(columnNamingStrategy.resolveColumnName(field.getName()));
            });
            builder.append(")");
        }

        builder.append(")");

        return builder.build();
    }

    @Override
    public String getAlterTableStatement(TableDescriptor table, SinkRecordDescriptor record, Set<String> missingFields) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("ALTER TABLE ");
        builder.append(table.getId().getTableName());
        builder.append(" ");
        builder.appendList(" ", missingFields, (name) -> {
            final FieldDescriptor field = record.getFields().get(name);
            final StringBuilder addColumnSpec = new StringBuilder();
            addColumnSpec.append("ADD ");
            addColumnSpec.append(toIdentifier(columnNamingStrategy.resolveColumnName(name)));
            addColumnSpec.append(" ").append(field.getTypeName());
            addColumnDefaultValue(field, addColumnSpec);

            addColumnSpec.append(field.getSchema().isOptional() ? " NULL" : " NOT NULL");
            return addColumnSpec.toString();
        });

        return builder.build();
    }

    @Override
    public String getInsertStatement(TableDescriptor table, SinkRecordDescriptor record) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("INSERT INTO ");

        builder.append(table.getId().getTableName());
        builder.append(" (");

        builder.appendLists(", ", record.getKeyFieldNames(), record.getNonKeyFieldNames(), (name) -> columnNameFromField(name, record));

        builder.append(") VALUES (");

        builder.appendLists(", ", record.getKeyFieldNames(), record.getNonKeyFieldNames(), (name) -> record.getFields().get(name).getQueryBinding());

        builder.append(")");

        return builder.build();
    }

    @Override
    public String getUpsertStatement(TableDescriptor table, SinkRecordDescriptor record) {
        throw new UnsupportedOperationException("Upsert configurations are not supported for this dialect");
    }

    @Override
    public String getUpdateStatement(TableDescriptor table, SinkRecordDescriptor record) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("UPDATE ");
        builder.append(table.getId().getTableName());
        builder.append(" SET ");
        builder.appendList(", ", record.getNonKeyFieldNames(), (name) -> columnNameEqualsBinding(name, record));

        if (!record.getKeyFieldNames().isEmpty()) {
            builder.append(" WHERE ");
            builder.appendList(" AND ", record.getKeyFieldNames(), (name) -> columnNameEqualsBinding(name, record));
        }

        return builder.build();
    }

    @Override
    public String getDeleteStatement(TableDescriptor table, SinkRecordDescriptor record) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("DELETE FROM ");
        builder.append(table.getId().getTableName());

        if (!record.getKeyFieldNames().isEmpty()) {
            builder.append(" WHERE ");
            builder.appendList(" AND ", record.getKeyFieldNames(), (name) -> columnNameEqualsBinding(name, record));
        }

        return builder.build();
    }

    @Override
    public int bindValue(FieldDescriptor field, NativeQuery<?> query, int startIndex, Object value) {
        LOGGER.trace("Bind field '{}' at position {}: {}", field.getName(), startIndex, value);
        field.bind(query, startIndex, value);
        return 1;
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
    public Type getSchemaType(Schema schema) {
        if (!Objects.isNull(schema.name())) {
            final Type type = typeRegistry.get(schema.name());
            if (!Objects.isNull(type)) {
                return type;
            }
        }
        if (!Objects.isNull(schema.parameters())) {
            final String columnType = schema.parameters().get("__debezium.source.column.type");
            if (!Objects.isNull(columnType)) {
                final Type type = typeRegistry.get(columnType);
                // We explicitly test whether the returned type is an AbstractConnectSchemaType because there
                // are use cases when column propagation is enabled and the source's column type may also map
                // directly to a raw Kafka schema type, i.e. INT8 from PostgreSQL. This prevents accidentally
                // resolving the dialect column type to a raw schema type.
                //
                // So in other words, if the column type is INT8 and the only registration is for the raw
                // schema type provided by Kafka Connect, this will not use that type and will fallback to
                // the lookup below based on the schema type's name, which in this case is also INT8.
                if (!Objects.isNull(type) && !(type instanceof AbstractConnectSchemaType)) {
                    return type;
                }
            }
        }

        final Type type = typeRegistry.get(schema.type().name());
        if (!Objects.isNull(type)) {
            return type;
        }

        throw new ConnectException(String.format("Failed to resolve column type for schema: %s (%s)", schema.type(), schema.name()));
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
    public String getTypeName(int jdbcType) {
        return ddlTypeRegistry.getTypeName(jdbcType, dialect);
    }

    @Override
    public String getTypeName(int jdbcType, Size size) {
        return ddlTypeRegistry.getTypeName(jdbcType, size);
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
    public String getFormattedDate(ZonedDateTime value) {
        return String.format("'%s'", DATE_FORMATTER.format(value));
    }

    @Override
    public String getFormattedTime(ZonedDateTime value) {
        return String.format("'%s'", DateTimeFormatter.ISO_TIME.format(value));
    }

    @Override
    public String getFormattedTimeWithTimeZone(String value) {
        return String.format("'%s'", value);
    }

    @Override
    public String getFormattedDateTime(ZonedDateTime value) {
        return String.format("'%s'", DateTimeFormatter.ISO_DATE_TIME.format(value));
    }

    @Override
    public String getFormattedDateTimeWithNanos(ZonedDateTime value) {
        return getFormattedDateTime(value);
    }

    @Override
    public String getFormattedTimestamp(ZonedDateTime value) {
        return String.format("'%s'", DateTimeFormatter.ISO_ZONED_DATE_TIME.format(value));
    }

    @Override
    public String getFormattedTimestampWithTimeZone(String value) {
        return String.format("'%s'", value);
    }

    protected String getTypeName(int jdbcType, int length) {
        return getTypeName(jdbcType, Size.length(length));
    }

    protected void registerTypes() {
        // todo: Need to add support for these data types
        //
        // Oracle and PostgreSQL:
        // io.debezium.time.interval - partially supported with PostgreSQL
        // io.debezium.time.MicroDuration - partially supported with PostgreSQL
        // io.debezium.time.NAnoDuration - not supported; emitted by Cassandra
        //
        // MySQL and PostgreSQL:
        // io.debezium.data.geometry.Geography - not yet supported
        // io.debezium.data.geometry.Geometry - not yet supported
        // io.debezium.data.geometry.Point - not yet supported

        // Supported common Debezium data types
        registerType(DateType.INSTANCE);
        registerType(TimeType.INSTANCE);
        registerType(MicroTimeType.INSTANCE);
        registerType(TimestampType.INSTANCE);
        registerType(MicroTimestampType.INSTANCE);
        registerType(NanoTimeType.INSTANCE);
        registerType(NanoTimestampType.INSTANCE);
        registerType(ZonedTimeType.INSTANCE);
        registerType(ZonedTimestampType.INSTANCE);
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
    }

    protected void registerType(Type type) {
        for (String key : type.getRegistrationKeys()) {
            final Type existing = typeRegistry.put(key, type);
            if (existing != null) {
                LOGGER.debug("Type replaced [{}]: {} -> {}", key, existing.getClass().getName(), type.getClass().getName());
            }
            else {
                LOGGER.debug("Type registered [{}]: {}", key, type.getClass().getName());
            }
        }
    }

    protected ColumnNamingStrategy getColumnNamingStrategy() {
        return columnNamingStrategy;
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
            final String defaultValue = field.getType().getDefaultValueBinding(this, field.getSchema(), field.getSchema().defaultValue());
            // final String defaultValue = resolveColumnDefaultValue(field, field.getSchema().defaultValue());
            if (defaultValue != null) {
                columnSpec.append(" DEFAULT ").append(defaultValue);
            }
        }
    }

    protected String columnQueryBindingFromField(String fieldName, SinkRecordDescriptor record) {
        return record.getFields().get(fieldName).getQueryBinding();
    }

    protected String columnNameFromField(String fieldName, SinkRecordDescriptor record) {
        final FieldDescriptor field = record.getFields().get(fieldName);
        final String columnName = getColumnNamingStrategy().resolveColumnName(field.getName());
        return getIdentifierHelper().toIdentifier(columnName, getConfig().isQuoteIdentifiers()).render(dialect);
    }

    protected String columnNameFromField(String fieldName, String prefix, SinkRecordDescriptor record) {
        return prefix + columnNameFromField(fieldName, record);
    }

    protected String toIdentifier(String text) {
        final Identifier identifier = getIdentifierHelper().toIdentifier(text, getConfig().isQuoteIdentifiers());
        return identifier != null ? identifier.render(dialect) : text;
    }

    protected String toIdentifier(TableId tableId) {
        final boolean quoted = getConfig().isQuoteIdentifiers();
        final Identifier catalog = getIdentifierHelper().toIdentifier(tableId.getCatalogName(), quoted);
        final Identifier schema = getIdentifierHelper().toIdentifier(tableId.getSchemaName(), quoted);
        final Identifier table = getIdentifierHelper().toIdentifier(tableId.getTableName(), quoted);

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

    protected boolean isIdentifierUppercaseWhenNotQuoted() {
        return false;
    }

    private String columnNameEqualsBinding(String fieldName, SinkRecordDescriptor record) {
        final FieldDescriptor field = record.getFields().get(fieldName);
        return toIdentifier(columnNamingStrategy.resolveColumnName(fieldName)) + "=" + field.getQueryBinding();
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
