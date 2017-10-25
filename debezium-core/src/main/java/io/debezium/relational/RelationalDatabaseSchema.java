/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import io.debezium.schema.DatabaseSchema;

public interface RelationalDatabaseSchema extends DatabaseSchema {

    Table getTable(TableId id);
}
