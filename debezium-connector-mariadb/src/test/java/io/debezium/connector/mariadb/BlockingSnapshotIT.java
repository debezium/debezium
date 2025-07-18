/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.connector.binlog.BinlogBlockingSnapshotIT;
import io.debezium.connector.binlog.junit.BinlogDatabaseVersionResolver;

/**
 * @author Chris Cranford
 */
public class BlockingSnapshotIT extends BinlogBlockingSnapshotIT<MariaDbConnector> implements MariaDbCommon {
    @Override
    protected String getDdlString(BinlogDatabaseVersionResolver databaseVersionResolver) {
        String charSetClause = "DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        if (databaseVersionResolver.getVersion().isLessThan(11, 7, 0)) {
            charSetClause = "DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci";
        }

        return "CREATE TABLE `b` (\n" +
                "  `pk` int(11) NOT NULL AUTO_INCREMENT,\n" +
                "  `aa` int(11) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`pk`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=1001 " + charSetClause;
    }
}
