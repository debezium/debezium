/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

enum AlterType {
    ADD_COLUMN,
    ADD_COLUMNS,
    CHANGE_COLUMN,
    MODIFY_COLUMN,
    DROP_COLUMN,
    RENAME_TABLE,
    ALTER_DEFAULT,
    ADD_PRIMARY_KEY,
    DROP_PRIMARY_KEY,
    ADD_UNIQUE_KEY,
    RENAME_COLUMN,
    CONVERT_CHARSET,
    ADD_CONSTRAINT,
    OTHER
}
