/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.valueholder;

/**
 * This class is a placeholder of DDL data
 *
 */
public interface LogMinerDdlEntry {
    /**
     * @return text of the DDL statement
     */
    String getDdlText();

    /**
     * @return string such as "CREATE TABLE", "ALTER TABLE", "DROP TABLE"
     */
    String getCommandType();
}
