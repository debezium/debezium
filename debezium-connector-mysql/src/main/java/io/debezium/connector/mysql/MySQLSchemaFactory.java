/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.schema.SchemaFactory;

public class MySQLSchemaFactory extends SchemaFactory {

    public MySQLSchemaFactory() {
        super();
    }

    private static final MySQLSchemaFactory mysqlSchemaFactoryObject = new MySQLSchemaFactory();

    public static MySQLSchemaFactory get() {
        return mysqlSchemaFactoryObject;
    }
}
