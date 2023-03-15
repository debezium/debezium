/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.mongodb.converters.MongoDbRecordParser;
import io.debezium.converters.spi.RecordParser;
import io.debezium.transforms.spi.QualifiedTableNameResolver;

public class MongoDbQualifiedTableNameResolver extends MongoDbAbstractRecordParserProvider implements QualifiedTableNameResolver {
    public static final String FULLY_QUALIFIED_NAME_FORMAT = "%s.%s";

    @Override
    public String resolve(RecordParser recordParser) {
        return String.format(FULLY_QUALIFIED_NAME_FORMAT,
                recordParser.getMetadata(AbstractSourceInfo.DATABASE_NAME_KEY),
                recordParser.getMetadata(MongoDbRecordParser.COLLECTION));
    }
}
