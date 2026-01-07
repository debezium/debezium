/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;

import io.debezium.connector.binlog.BinlogVectorIT;
import io.debezium.junit.SkipWhenDatabaseVersion;

/**
 * @author Jiri Pechanec
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 9, minor = 0, reason = "VECTOR datatype not added until MySQL 9.0")
public class MySqlVectorIT extends BinlogVectorIT<MySqlConnector> implements MySqlCommon {

    public MySqlVectorIT() {
        super("vector_test");
    }

    @Override
    protected String getShouldConsumeAllEventsFromDatabaseUsingStreamingSql() {
        return "INSERT INTO dbz_8157 VALUES (default, string_to_vector('[10.1,10.2]'),string_to_vector('[20.1,20.2]'),string_to_vector('[30.1,30.2]'));";
    }

}
