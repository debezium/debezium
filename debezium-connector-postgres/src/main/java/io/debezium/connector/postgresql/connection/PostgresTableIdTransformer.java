/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import io.debezium.relational.TableIdTransformer;
import io.debezium.relational.TableIdTransformerQuoted;

public final class PostgresTableIdTransformer {

    public static final TableIdTransformer INSTANCE = new TableIdTransformerQuoted("\"");

}
