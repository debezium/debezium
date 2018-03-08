/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.relational.ddl;

import io.debezium.relational.Tables;

/**
 * A parser interface for DDL statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public interface DdlParser {

    void parse(String ddlContent, Tables databaseTables);

    void setCurrentDatabase(String databaseName);

    void setCurrentSchema(String schemaName);
}
