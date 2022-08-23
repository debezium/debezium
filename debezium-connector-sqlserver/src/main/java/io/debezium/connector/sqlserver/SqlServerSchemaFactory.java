/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import io.debezium.schema.SchemaFactory;

public class SqlServerSchemaFactory extends SchemaFactory {

    public SqlServerSchemaFactory() {
        super();
    }

    private static final SqlServerSchemaFactory sqlServerSchemaFactoryObject = new SqlServerSchemaFactory();

    public static SqlServerSchemaFactory get() {
        return sqlServerSchemaFactoryObject;
    }
}
