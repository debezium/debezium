/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc.history;

import java.util.List;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.jdbc.JdbcCommonConfig;
import io.debezium.util.Collect;

/**
 * Configuration options specific for JDBC schema history storage.
 *
 * @author Jiri Pechanec
 *
 */
public class JdbcSchemaHistoryConfig extends JdbcCommonConfig {

    private static final String DEFAULT_TABLE_NAME = "debezium_database_history";
    private static final Field PROP_TABLE_NAME = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "schema.history.table.name")
            .withDescription("The database key that will be used to store the database schema history")
            .withDefault(DEFAULT_TABLE_NAME);

    /**
     * Table that will store database history.
     * id - Unique identifier(UUID)
     * history_data - Schema history data.
     * history_data_seq - Schema history part sequence number.
     * record_insert_ts - Timestamp when the record was inserted
     * record_insert_seq - Sequence number(Incremented for every record inserted)
     */
    private static final String DEFAULT_TABLE_DDL = "CREATE TABLE %s" +
            "(" +
            "id VARCHAR(36) NOT NULL," +
            "history_data VARCHAR(65000)," +
            "history_data_seq INTEGER," +
            "record_insert_ts TIMESTAMP NOT NULL," +
            "record_insert_seq INTEGER NOT NULL" +
            ")";

    /**
     * Field that will store the CREATE TABLE DDL for schema history.
     */
    public static final Field PROP_TABLE_DDL = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "schema.history.table.ddl")
            .withDescription("CREATE TABLE statement for schema history table")
            .withDefault(DEFAULT_TABLE_DDL);

    private static final String DEFAULT_TABLE_SELECT = "SELECT id, history_data, history_data_seq FROM %s"
            + " ORDER BY record_insert_ts, record_insert_seq, id, history_data_seq";

    /**
     * Field that will store the Schema history SELECT query.
     */
    public static final Field PROP_TABLE_SELECT = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "schema.history.table.select")
            .withDescription("SELECT statement to get the schema history from a database table")
            .withDefault(DEFAULT_TABLE_SELECT);

    private static final String DEFAULT_TABLE_DATA_EXISTS_SELECT = "SELECT * FROM %s LIMIT 1";

    /**
     *  Field that will store the Schema history SELECT query to check existence of the table.
     */
    public static final Field PROP_TABLE_DATA_EXISTS_SELECT = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "schema.history.table.select")
            .withDescription("SELECT statement to check existence of the storage table")
            .withDefault(DEFAULT_TABLE_DATA_EXISTS_SELECT);

    private static final String TABLE_INSERT = "INSERT INTO %s VALUES ( ?, ?, ?, ?, ? )";

    private String tableName;
    private String tableCreate;
    private String tableSelect;
    private String tableDataExistsSelect;
    private String tableInsert;

    public JdbcSchemaHistoryConfig(Configuration config) {
        super(config, SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING);
    }

    @Override
    protected void init(Configuration config) {
        super.init(config);
        this.tableName = config.getString(PROP_TABLE_NAME);
        this.tableCreate = String.format(config.getString(PROP_TABLE_DDL), tableName);
        this.tableSelect = String.format(config.getString(PROP_TABLE_SELECT), tableName);
        this.tableDataExistsSelect = String.format(config.getString(PROP_TABLE_DATA_EXISTS_SELECT), tableName);
        this.tableInsert = String.format(TABLE_INSERT, tableName);
    }

    @Override
    protected List<Field> getAllConfigurationFields() {
        List<Field> fields = Collect.arrayListOf(PROP_TABLE_NAME, PROP_TABLE_DDL, PROP_TABLE_SELECT, PROP_TABLE_DATA_EXISTS_SELECT);
        fields.addAll(super.getAllConfigurationFields());
        return fields;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableCreate() {
        return tableCreate;
    }

    public String getTableSelect() {
        return tableSelect;
    }

    public String getTableDataExistsSelect() {
        return tableDataExistsSelect;
    }

    public String getTableInsert() {
        return tableInsert;
    }
}