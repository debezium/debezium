/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.ResultSet;
import java.sql.SQLException;

import io.debezium.relational.Column;
import io.debezium.relational.Table;

/**
 * Decode MySQL return value according to different protocols.
 *
 * Normally, MySQL uses "Text protocol" to return a value. When set `useCursorFetch=true`,
 * `useServerPrepStmts` is consequently also set to `true`, setting `useServerPrepStmts=true`
 * internally causes the MySQL protocol to change from "Text protocol" to "Binary Protocol".
 *
 * @see MySqlBinaryProtocolFieldReader
 * @see MySqlTextProtocolFieldReader
 * @see <a href="https://issues.redhat.com/browse/DBZ-3238">DBZ-3238</a>
 * @author yangjie
 */
public interface MySqlFieldReader {

    /**
     * read field from ResultSet according to different protocols
     */
    Object readField(ResultSet rs, int fieldNo, Column actualColumn, Table actualTable) throws SQLException;
}
