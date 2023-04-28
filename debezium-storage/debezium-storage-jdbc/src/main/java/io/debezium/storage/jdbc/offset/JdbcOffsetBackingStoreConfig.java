/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc.offset;

import java.util.List;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.storage.jdbc.JdbcCommonConfig;
import io.debezium.util.Collect;

/**
 * Configuration options specific for JDBC offset storage.
 *
 * @author Jiri Pechanec
 *
 */
public class JdbcOffsetBackingStoreConfig extends JdbcCommonConfig {

    public static final String OFFSET_STORAGE_PREFIX = "offset.storage.";
    public static final String PROP_PREFIX = OFFSET_STORAGE_PREFIX + CONFIGURATION_FIELD_PREFIX_STRING;

    public static final String DEFAULT_TABLE_NAME = "debezium_offset_storage";
    public static final Field PROP_TABLE_NAME = Field.create(PROP_PREFIX + "offset_table_name")
            .withDescription("Name of the table to store offsets")
            .withDefault(DEFAULT_TABLE_NAME);

    /**
     * JDBC Offset storage CREATE TABLE syntax.
     */
    public static final String DEFAULT_TABLE_DDL = "CREATE TABLE %s(id VARCHAR(36) NOT NULL, " +
            "offset_key VARCHAR(1255), offset_val VARCHAR(1255)," +
            "record_insert_ts TIMESTAMP NOT NULL," +
            "record_insert_seq INTEGER NOT NULL" +
            ")";

    /**
     * The JDBC table that will store offset information.
     * id - UUID
     * offset_key - Offset Key
     * offset_val - Offset value
     * record_insert_ts - Timestamp when the record was inserted
     * record_insert_seq - Sequence number of record
     */
    public static final Field PROP_TABLE_DDL = Field.create(PROP_PREFIX + "offset_table_ddl")
            .withDescription("Create table syntax for offset jdbc table")
            .withDefault(DEFAULT_TABLE_DDL);

    public static final String DEFAULT_TABLE_SELECT = "SELECT id, offset_key, offset_val FROM %s " +
            "ORDER BY record_insert_ts, record_insert_seq";
    public static final Field PROP_TABLE_SELECT = Field.create(PROP_PREFIX + "offset_table_select")
            .withDescription("Select syntax to get offset data from jdbc table")
            .withDefault(DEFAULT_TABLE_SELECT);

    public static final String TABLE_INSERT = "INSERT INTO %s VALUES ( ?, ?, ?, ?, ? )";

    public static final String TABLE_DELETE = "DELETE FROM %s";

    private String tableCreate;
    private String tableSelect;
    private String tableDelete;
    private String tableInsert;
    private String tableName;

    public JdbcOffsetBackingStoreConfig(Configuration config) {
        super(config, OFFSET_STORAGE_PREFIX);
    }

    @Override
    protected void init(Configuration config) {
        super.init(config);
        this.tableName = config.getString(PROP_TABLE_NAME);
        this.tableCreate = String.format(config.getString(PROP_TABLE_DDL), tableName);
        this.tableSelect = String.format(config.getString(PROP_TABLE_SELECT), tableName);
        this.tableInsert = String.format(TABLE_INSERT, tableName);
        this.tableDelete = String.format(TABLE_DELETE, tableName);
    }

    @Override
    protected List<Field> getAllConfigurationFields() {
        List<Field> fields = Collect.arrayListOf(PROP_TABLE_NAME, PROP_TABLE_DDL, PROP_TABLE_SELECT);
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

    public String getTableDelete() {
        return tableDelete;
    }

    public String getTableInsert() {
        return tableInsert;
    }
}