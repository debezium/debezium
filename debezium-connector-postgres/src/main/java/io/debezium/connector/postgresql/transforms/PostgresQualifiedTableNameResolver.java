package io.debezium.connector.postgresql.transforms;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.spi.RecordParser;
import io.debezium.transforms.spi.QualifiedTableNameResolver;

public class PostgresQualifiedTableNameResolver extends PostgresAbstractRecordParserProvider implements QualifiedTableNameResolver {
    public static final String FULLY_QUALIFIED_NAME_FORMAT = "%s.%s";

    @Override
    public String resolve(RecordParser recordParser) {
        return String.format(FULLY_QUALIFIED_NAME_FORMAT,
                recordParser.getMetadata(AbstractSourceInfo.SCHEMA_NAME_KEY),
                recordParser.getMetadata(AbstractSourceInfo.TABLE_NAME_KEY));
    }
}
