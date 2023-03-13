package io.debezium.connector.mysql.transforms;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.spi.RecordParser;
import io.debezium.transforms.spi.QualifiedTableNameResolver;

public class MySqlQualifiedTableNameResolver extends MySqlAbstractRecordParserProvider implements QualifiedTableNameResolver {
    public static final String FULLY_QUALIFIED_NAME_FORMAT = "%s.%s";

    @Override
    public String resolve(RecordParser recordParser) {
        return String.format(FULLY_QUALIFIED_NAME_FORMAT,
                recordParser.getMetadata(AbstractSourceInfo.DATABASE_NAME_KEY),
                recordParser.getMetadata(AbstractSourceInfo.TABLE_NAME_KEY));
    }
}
