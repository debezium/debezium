/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE;

import java.sql.SQLException;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.query.NativeQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.InsertMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.PrimaryKeyMode;
import io.debezium.connector.jdbc.SinkRecordDescriptor.FieldDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectResolver;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.sink.spi.ChangeEventSink;

/**
 * A {@link ChangeEventSink} for a JDBC relational database.
 *
 * @author Chris Cranford
 */
public class JdbcChangeEventSink implements ChangeEventSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcChangeEventSink.class);

    private final JdbcSinkConnectorConfig config;
    private final SessionFactory sessionFactory;
    private final DatabaseDialect dialect;
    private final StatelessSession session;

    public JdbcChangeEventSink(JdbcSinkConnectorConfig config) {
        this.config = config;
        this.sessionFactory = config.getHibernateConfiguration().buildSessionFactory();

        this.dialect = DatabaseDialectResolver.resolve(config, sessionFactory);
        this.session = this.sessionFactory.openStatelessSession();

        final DatabaseVersion version = this.dialect.getVersion();
        LOGGER.info("Database version {}.{}.{}", version.getMajor(), version.getMinor(), version.getMicro());
    }

    @Override
    public void execute(SinkRecord record) {
        try {
            // Examine the sink record and prepare a descriptor
            final SinkRecordDescriptor descriptor = SinkRecordDescriptor.builder()
                    .withPrimaryKeyMode(config.getPrimaryKeyMode())
                    .withPrimaryKeyFields(config.getPrimaryKeyFields())
                    .withSinkRecord(record)
                    .withDialect(dialect)
                    .build();

            final TableDescriptor table = checkAndApplyTableChangesIfNeeded(record, descriptor);
            write(table, descriptor);
        }
        catch (Exception e) {
            throw new ConnectException("Failed to process a sink record", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (session != null && session.isOpen()) {
            LOGGER.info("Closing session.");
            session.close();
        }
        else {
            LOGGER.info("Session already closed.");
        }

        if (sessionFactory != null && sessionFactory.isOpen()) {
            LOGGER.info("Closing the session factory");
            sessionFactory.close();
        }
        else {
            LOGGER.info("Session factory already closed");
        }
    }

    private TableDescriptor checkAndApplyTableChangesIfNeeded(SinkRecord record, SinkRecordDescriptor descriptor) throws SQLException {
        final String tableName = config.getTableNamingStrategy().resolveTableName(config, record);
        if (!hasTable(tableName)) {
            // Table does not exist, lets attempt to create it.
            try {
                return createTable(tableName, descriptor);
            }
            catch (SQLException ce) {
                // It's possible the table may have been created in the interim, so try to alter.
                LOGGER.warn("Table creation failed for '{}', attempting to alter the table", tableName, ce);
                try {
                    return alterTableIfNeeded(tableName, descriptor);
                }
                catch (SQLException ae) {
                    // The alter failed, hard stop.
                    LOGGER.error("Failed to alter the table '{}'.", tableName, ae);
                    throw ae;
                }
            }
        }
        else {
            // Table exists, lets attempt to alter it if necessary.
            try {
                return alterTableIfNeeded(tableName, descriptor);
            }
            catch (SQLException ae) {
                LOGGER.error("Failed to alter the table '{}'.", tableName, ae);
                throw ae;
            }
        }
    }

    private boolean hasTable(String tableName) {
        return session.doReturningWork((connection) -> dialect.tableExists(connection, tableName));
    }

    private TableDescriptor readTable(String tableName) {
        return session.doReturningWork((connection) -> dialect.readTable(connection, tableName));
    }

    private TableDescriptor createTable(String tableName, SinkRecordDescriptor record) throws SQLException {
        LOGGER.debug("Attempting to create table '{}'.", tableName);

        if (NONE.equals(config.getSchemaEvolutionMode())) {
            LOGGER.warn("Table '{}' cannot be created because schema evolution is disabled.", tableName);
            throw new SQLException("Cannot create table " + tableName + " because schema evolution is disabled");
        }

        Transaction transaction = session.beginTransaction();
        try {
            final String createSql = dialect.getCreateTableStatement(record, tableName);
            LOGGER.trace("SQL: {}", createSql);
            session.createNativeQuery(createSql, Object.class).executeUpdate();
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }

        return readTable(tableName);
    }

    private TableDescriptor alterTableIfNeeded(String tableName, SinkRecordDescriptor record) throws SQLException {
        LOGGER.debug("Attempting to alter table '{}'.", tableName);

        if (!hasTable(tableName)) {
            LOGGER.error("Table '{}' does not exist and cannot be altered.", tableName);
            throw new SQLException("Could not find table: " + tableName);
        }

        // Resolve table metadata from the database
        final TableDescriptor table = readTable(tableName);

        // Delegating to dialect to deal with database case sensitivity.
        Set<String> missingFields = dialect.resolveMissingFields(record, table);
        if (missingFields.isEmpty()) {
            // There are no missing fields, simply return
            // todo: should we check column type changes or default value changes?
            return table;
        }

        LOGGER.debug("The follow fields are missing in the table: {}", missingFields);
        for (String missingFieldName : missingFields) {
            final FieldDescriptor fieldDescriptor = record.getFields().get(missingFieldName);
            if (!fieldDescriptor.getSchema().isOptional() && fieldDescriptor.getSchema().defaultValue() == null) {
                throw new SQLException(String.format(
                        "Cannot ALTER table '%s' because field '%s' is not optional but has no default value",
                        tableName, fieldDescriptor.getName()));
            }
        }

        if (NONE.equals(config.getSchemaEvolutionMode())) {
            LOGGER.warn("Table '{}' cannot be altered because schema evolution is disabled.", tableName);
            throw new SQLException("Cannot alter table " + tableName + " because schema evolution is disabled");
        }

        Transaction transaction = session.beginTransaction();
        try {
            final String alterSql = dialect.getAlterTableStatement(table, record, missingFields);
            LOGGER.trace("SQL: {}", alterSql);
            session.createNativeQuery(alterSql, Object.class).executeUpdate();
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }

        return readTable(tableName);
    }

    private void write(TableDescriptor table, SinkRecordDescriptor record) throws SQLException {
        if (!record.isDebeziumSinkRecord()) {
            final Struct value = (Struct) record.getRawRecord().value();
            if (value != null) {
                if (InsertMode.INSERT.equals(config.getInsertMode())) {
                    final String sql = dialect.getInsertStatement(table, record);
                    writeInsert(sql, record);
                }
                else if (InsertMode.UPSERT.equals(config.getInsertMode())) {
                    if (record.getKeyFieldNames().isEmpty()) {
                        throw new ConnectException("Cannot write to table " + table.getId().getTableName() + " with no key fields defined.");
                    }
                    final String sql = dialect.getUpsertStatement(table, record);
                    writeUpsert(sql, record);
                }
                else if (InsertMode.UPDATE.equals(config.getInsertMode())) {
                    final String sql = dialect.getUpdateStatement(table, record);
                    writeUpdate(sql, record);
                }
            }
            else {
                final String sql = dialect.getDeleteStatement(table, record);
                writeDelete(sql, record);
            }
        }
        else {
            // Complex Debezium data structure
            final Struct value = (Struct) record.getRawRecord().value();
            final Operation operation = Operation.forCode(value.getString(Envelope.FieldName.OPERATION));

            if (!Operation.DELETE.equals(operation)) {
                if (InsertMode.INSERT.equals(config.getInsertMode())) {
                    final String sql = dialect.getInsertStatement(table, record);
                    writeInsert(sql, record);
                }
                else if (InsertMode.UPSERT.equals(config.getInsertMode())) {
                    if (record.getKeyFieldNames().isEmpty()) {
                        throw new ConnectException("Cannot write to table " + table.getId().getTableName() + " with no key fields defined.");
                    }
                    final String sql = dialect.getUpsertStatement(table, record);
                    writeUpsert(sql, record);
                }
                else if (InsertMode.UPDATE.equals(config.getInsertMode())) {
                    final String sql = dialect.getUpdateStatement(table, record);
                    writeUpdate(sql, record);
                }
            }
            else {
                final String sql = dialect.getDeleteStatement(table, record);
                writeDelete(sql, record);
            }
        }
    }

    private void writeInsert(String sql, SinkRecordDescriptor record) throws SQLException {
        final Transaction transaction = session.beginTransaction();
        try {
            LOGGER.trace("SQL: {}", sql);
            final NativeQuery<?> query = session.createNativeQuery(sql, Object.class);
            int index = bindKeyValuesToQuery(record, query, 1);
            bindNonKeyValuesToQuery(record, query, index);

            final int result = query.executeUpdate();
            if (result != 1) {
                throw new SQLException("Failed to insert row from table");
            }

            transaction.commit();
        }
        catch (SQLException e) {
            transaction.rollback();
            throw e;
        }
    }

    private void writeUpsert(String sql, SinkRecordDescriptor record) throws SQLException {
        final Transaction transaction = session.beginTransaction();
        try {
            LOGGER.trace("SQL: {}", sql);
            final NativeQuery<?> query = session.createNativeQuery(sql, Object.class);
            int index = bindKeyValuesToQuery(record, query, 1);
            bindNonKeyValuesToQuery(record, query, index);

            query.executeUpdate();
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }
    }

    private void writeUpdate(String sql, SinkRecordDescriptor record) throws SQLException {
        final Transaction transaction = session.beginTransaction();
        try {
            LOGGER.trace("SQL: {}", sql);
            final NativeQuery<?> query = session.createNativeQuery(sql, Object.class);
            int index = bindNonKeyValuesToQuery(record, query, 1);
            bindKeyValuesToQuery(record, query, index);

            query.executeUpdate();
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }
    }

    private void writeDelete(String sql, SinkRecordDescriptor record) throws SQLException {
        if (!config.isDeleteEnabled()) {
            LOGGER.info("Deletes are not enabled, skipping delete for topic '{}'", record.getTopicName());
            return;
        }
        final Transaction transaction = session.beginTransaction();
        try {
            LOGGER.info("SQL: {}", sql);
            final NativeQuery<?> query = session.createNativeQuery(sql, Object.class);
            bindKeyValuesToQuery(record, query, 1);

            query.executeUpdate();
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }
    }

    private int bindFieldValue(NativeQuery<?> query, FieldDescriptor field, Object value, int index) {
        return index + dialect.bindValue(field, query, index, value);
    }

    private int bindKeyValuesToQuery(SinkRecordDescriptor record, NativeQuery<?> query, int index) {
        if (PrimaryKeyMode.KAFKA.equals(config.getPrimaryKeyMode())) {
            query.setParameter(index++, record.getTopicName());
            query.setParameter(index++, record.getRawRecord().kafkaPartition());
            query.setParameter(index++, record.getRawRecord().kafkaOffset());
        }
        else if (!record.getKeyFieldNames().isEmpty()) {
            // There are key fields, either mapped to RECORD_KEY or RECORD_VALUE potentially
            if (PrimaryKeyMode.RECORD_KEY.equals(config.getPrimaryKeyMode())) {
                final Schema keySchema = record.getRawRecord().keySchema();
                if (keySchema != null && Schema.Type.STRUCT.equals(keySchema.type())) {
                    for (String fieldName : record.getKeyFieldNames()) {
                        final FieldDescriptor field = record.getFields().get(fieldName);
                        final Object fieldValue = ((Struct) record.getRawRecord().key()).get(fieldName);
                        index = bindFieldValue(query, field, fieldValue, index);
                    }
                }
                else {
                    throw new ConnectException("Cannot bind primary key with no struct-based key");
                }
            }
            else if (PrimaryKeyMode.RECORD_VALUE.equals(config.getPrimaryKeyMode())) {
                final Schema valueSchema = record.getRawRecord().valueSchema();
                if (valueSchema != null && Schema.Type.STRUCT.equals(valueSchema.type())) {
                    final Struct source;
                    if (record.isDebeziumSinkRecord()) {
                        source = ((Struct) record.getRawRecord().value()).getStruct(Envelope.FieldName.AFTER);
                    }
                    else {
                        source = ((Struct) record.getRawRecord().value());
                    }

                    for (String fieldName : record.getKeyFieldNames()) {
                        final FieldDescriptor field = record.getFields().get(fieldName);
                        final Object fieldValue = source.get(fieldName);
                        index = bindFieldValue(query, field, fieldValue, index);
                    }
                }
                else {
                    throw new ConnectException("Cannot bind primary key with a non-struct value");
                }
            }
        }
        return index;
    }

    private int bindNonKeyValuesToQuery(SinkRecordDescriptor record, NativeQuery<?> query, int index) {
        final Struct struct;
        if (record.isDebeziumSinkRecord()) {
            struct = ((Struct) record.getRawRecord().value()).getStruct(Envelope.FieldName.AFTER);
        }
        else {
            struct = ((Struct) record.getRawRecord().value());
        }

        for (String fieldName : record.getNonKeyFieldNames()) {
            final FieldDescriptor field = record.getFields().get(fieldName);
            index += dialect.bindValue(field, query, index, struct.get(fieldName));
        }

        return index;
    }

}
