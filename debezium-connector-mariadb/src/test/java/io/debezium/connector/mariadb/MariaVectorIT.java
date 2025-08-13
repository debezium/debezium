/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static io.debezium.junit.EqualityCheck.LESS_THAN;

import io.debezium.connector.binlog.BinlogVectorIT;
import io.debezium.junit.SkipWhenDatabaseVersion;

@SkipWhenDatabaseVersion(check = LESS_THAN, major = 11, minor = 7, reason = "VECTOR datatype not added until MySQL 9.0")
public class MariaVectorIT extends BinlogVectorIT<MariaDbConnector> implements MariaDbCommon {

    public MariaVectorIT() {
        super("maria_vector_test");
    }

    @Override
    protected String getShouldConsumeAllEventsFromDatabaseUsingStreamingSql() {
        return "INSERT INTO dbz_8157 VALUES (default, Vec_FromText('[10.1,10.2]'),Vec_FromText('[20.1,20.2]'),Vec_FromText('[30.1,30.2]'));";
    }

}