/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.jdbc;

import io.debezium.connector.binlog.jdbc.BinlogFieldReader;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.relational.Column;

/**
 * An abstract implementation of {@link BinlogFieldReader} for MySQL.<p></p>
 *
 * Normally MySQL uses the "Text protocol" to return values; however, when {@code useCursorFetch} is enabled,
 * the driver sets {@code useServerPrepStmts} to {@code true}, which internally causes MysQL to change from
 * the "Text Protocol" to the "Binary Protocol".
 *
 * @see io.debezium.connector.mysql.jdbc.MySqlTextProtocolFieldReader
 * @see MySqlBinaryProtocolFieldReader
 * @see <a href="https://issues.redhat.com/browse/DBZ-3238">DBZ-3238</a>
 *
 * @author Chris Cranford
 */
public abstract class AbstractFieldReader extends BinlogFieldReader {

    public AbstractFieldReader(MySqlConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    protected String getCharacterSet(Column column) {
        return getCharsetRegistry().getJavaEncodingForCharSet(column.charsetName());
    }
}
