/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.transforms;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.spi.RecordParser;
import io.debezium.transforms.spi.QualifiedTableNameResolver;

public class OracleQualifiedTableNameResolver extends OracleAbstractRecordParserProvider implements QualifiedTableNameResolver {
    public static final String FULLY_QUALIFIED_NAME_FORMAT = "%s.%s";

    @Override
    public String resolve(RecordParser recordParser) {
        return String.format(FULLY_QUALIFIED_NAME_FORMAT,
                recordParser.getMetadata(AbstractSourceInfo.SCHEMA_NAME_KEY),
                recordParser.getMetadata(AbstractSourceInfo.TABLE_NAME_KEY));
    }
}
