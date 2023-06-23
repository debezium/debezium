/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

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

import io.debezium.connector.jdbc.SinkRecordDescriptor.FieldDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectResolver;
import io.debezium.connector.jdbc.naming.TableNamingStrategy;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.connector.jdbc.relational.TableId;
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
    private final TableNamingStrategy tableNamingStrategy;

    public JdbcChangeEventSink(JdbcSinkConnectorConfig config) {
        this.config = config;
        this.sessionFactory = config.getHibernateConfiguration().buildSessionFactory();
        this.tableNamingStrategy = config.getTableNamingStrategy();

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

            String tableName = tableNamingStrategy.resolveTableName(config, record);
            if (tableName == null) {
                LOGGER.warn("Ignored to write record from topic '{}' partition '{}' offset '{}'", record.topic(), record.kafkaPartition(), record.kafkaOffset());
                return;
            }
            final TableId tableId = dialect.getTableId(tableName);
            final TableDescriptor table = checkAndApplyTableChangesIfNeeded(tableId, descriptor);
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

    private TableDescriptor checkAndApplyTableChangesIfNeeded(TableId tableId, SinkRecordDescriptor descriptor) throws SQLException {
        if (!hasTable(tableId)) {
            // Table does not exist, lets attempt to create it.
            try {
                return createTable(tableId, descriptor);
            }
            catch (SQLException ce) {
                // It's possible the table may have been created in the interim, so try to alter.
                LOGGER.warn("Table creation failed for '{}', attempting to alter the table", tableId.toFullIdentiferString(), ce);
                try {
                    return alterTableIfNeeded(tableId, descriptor);
                }
                catch (SQLException ae) {
                    // The alter failed, hard stop.
                    LOGGER.error("Failed to alter the table '{}'.", tableId.toFullIdentiferString(), ae);
                    throw ae;
                }
            }
        }
        else {
            // Table exists, lets attempt to alter it if necessary.
            try {
                return alterTableIfNeeded(tableId, descriptor);
            }
            catch (SQLException ae) {
                LOGGER.error("Failed to alter the table '{}'.", tableId.toFullIdentiferString(), ae);
                throw ae;
            }
        }
    }

    private boolean hasTable(TableId tableId) {
        return session.doReturningWork((connection) -> dialect.tableExists(connection, tableId));
    }

    private TableDescriptor readTable(TableId tableId) {
        return session.doReturningWork((connection) -> dialect.readTable(connection, tableId));
    }

    private TableDescriptor createTable(TableId tableId, SinkRecordDescriptor record) throws SQLException {
        LOGGER.debug("Attempting to create table '{}'.", tableId.toFullIdentiferString());

        if (NONE.equals(config.getSchemaEvolutionMode())) {
            LOGGER.warn("Table '{}' cannot be created because schema evolution is disabled.", tableId.toFullIdentiferString());
            throw new SQLException("Cannot create table " + tableId.toFullIdentiferString() + " because schema evolution is disabled");
        }

        Transaction transaction = session.beginTransaction();
        try {
            final String createSql = dialect.getCreateTableStatement(record, tableId);
            LOGGER.trace("SQL: {}", createSql);
            session.createNativeQuery(createSql, Object.class).executeUpdate();
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }

        return readTable(tableId);
    }

    private TableDescriptor alterTableIfNeeded(TableId tableId, SinkRecordDescriptor record) throws SQLException {
        LOGGER.debug("Attempting to alter table '{}'.", tableId.toFullIdentiferString());

        if (!hasTable(tableId)) {
            LOGGER.error("Table '{}' does not exist and cannot be altered.", tableId.toFullIdentiferString());
            throw new SQLException("Could not find table: " + tableId.toFullIdentiferString());
        }

        // Resolve table metadata from the database
        final TableDescriptor table = readTable(tableId);

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
                        tableId.toFullIdentiferString(), fieldDescriptor.getName()));
            }
        }

        if (NONE.equals(config.getSchemaEvolutionMode())) {
            LOGGER.warn("Table '{}' cannot be altered because schema evolution is disabled.", tableId.toFullIdentiferString());
            throw new SQLException("Cannot alter table " + tableId.toFullIdentiferString() + " because schema evolution is disabled");
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

        return readTable(tableId);
    }

    private void write(TableDescriptor table, SinkRecordDescriptor record) throws SQLException {
        if (!record.isDelete()) {
            switch (config.getInsertMode()) {
                case INSERT:
                    writeInsert(dialect.getInsertStatement(table, record), record);
                    break;
                case UPSERT:
                    if (record.getKeyFieldNames().isEmpty()) {
                        throw new ConnectException("Cannot write to table " + table.getId().getTableName() + " with no key fields defined.");
                    }
                    writeUpsert(dialect.getUpsertStatement(table, record), record);
                    break;
                case UPDATE:
                    writeUpdate(dialect.getUpdateStatement(table, record), record);
                    break;
            }
        }
        else {
            writeDelete(dialect.getDeleteStatement(table, record), record);
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
            LOGGER.debug("Deletes are not enabled, skipping delete for topic '{}'", record.getTopicName());
            return;
        }
        final Transaction transaction = session.beginTransaction();
        try {
            LOGGER.trace("SQL: {}", sql);
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

    private int bindKeyValuesToQuery(SinkRecordDescriptor record, NativeQuery<?> query, int index) {
        switch (config.getPrimaryKeyMode()) {
            case KAFKA:
                query.setParameter(index++, record.getTopicName());
                query.setParameter(index++, record.getPartition());
                query.setParameter(index++, record.getOffset());
                break;
            case RECORD_KEY:
            case RECORD_VALUE:
                final Struct keySource = record.getKeyStruct(config.getPrimaryKeyMode());
                if (keySource != null) {
                    index = bindFieldValuesToQuery(record, query, index, keySource, record.getKeyFieldNames());
                }
                break;
        }
        return index;
    }

    private int bindNonKeyValuesToQuery(SinkRecordDescriptor record, NativeQuery<?> query, int index) {
        return bindFieldValuesToQuery(record, query, index, record.getAfterStruct(), record.getNonKeyFieldNames());
    }

    private int bindFieldValuesToQuery(SinkRecordDescriptor record, NativeQuery<?> query, int index, Struct source, List<String> fields) {
        for (String fieldName : fields) {
            final FieldDescriptor field = record.getFields().get(fieldName);
            index += dialect.bindValue(field, query, index, source.get(fieldName));
        }
        return index;
    }

}
