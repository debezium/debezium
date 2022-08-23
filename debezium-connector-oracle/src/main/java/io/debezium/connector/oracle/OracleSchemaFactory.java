/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.schema.SchemaFactory;

public class OracleSchemaFactory extends SchemaFactory {

    public OracleSchemaFactory() {
        super();
    }

    private static final OracleSchemaFactory oracleSchemaFactoryObject = new OracleSchemaFactory();

    public static OracleSchemaFactory get() {
        return oracleSchemaFactoryObject;
    }

}
