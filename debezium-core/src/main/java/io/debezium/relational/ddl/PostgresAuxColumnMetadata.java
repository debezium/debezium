/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.relational.ddl;

/**
 * 
 * @author Raphael Costanzo Stefani <linux10000@gmail.com>.
 *
 */
public class PostgresAuxColumnMetadata {

    private Integer oid;
    private String tableCatalog;
    private String tableSchema;
    private String tableName;
    private String columnName;
    private String typeName;
    private Integer scale;

    public PostgresAuxColumnMetadata() {

    }

    public PostgresAuxColumnMetadata(Integer oid, String tableCatalog, String tableSchema, String tableName,
            String columnName, String typeName, Integer scale) {
        super();
        this.oid = oid;
        this.tableCatalog = tableCatalog;
        this.tableSchema = tableSchema;
        this.tableName = tableName;
        this.columnName = columnName;
        this.typeName = typeName;
        this.scale = scale;
    }


    public Integer getOid() {
        return oid;
    }

    public void setOid(Integer oid) {
        this.oid = oid;
    }

    public String getTableCatalog() {
        return tableCatalog;
    }

    public void setTableCatalog(String tableCatalog) {
        this.tableCatalog = tableCatalog;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }
    
}
