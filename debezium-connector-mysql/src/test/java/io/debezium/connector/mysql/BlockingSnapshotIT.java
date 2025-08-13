/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import io.debezium.connector.binlog.BinlogBlockingSnapshotIT;
import io.debezium.connector.binlog.junit.BinlogDatabaseVersionResolver;

public class BlockingSnapshotIT extends BinlogBlockingSnapshotIT<MySqlConnector> implements MySqlCommon {
    @Override
    protected String getDdlString(BinlogDatabaseVersionResolver databaseVersionResolver) {
        if (databaseVersionResolver.getVersion().getMajor() < MYSQL8) {
            return "CREATE TABLE `b` (\n" +
                    "  `pk` int(11) NOT NULL AUTO_INCREMENT,\n" +
                    "  `aa` int(11) DEFAULT NULL,\n" +
                    "  PRIMARY KEY (`pk`)\n" +
                    ") ENGINE=InnoDB AUTO_INCREMENT=1001 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci";
        }

        return "CREATE TABLE `b` (\n" +
                "  `pk` int NOT NULL AUTO_INCREMENT,\n" +
                "  `aa` int DEFAULT NULL,\n" +
                "  PRIMARY KEY (`pk`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=1001 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci";
    }
}
