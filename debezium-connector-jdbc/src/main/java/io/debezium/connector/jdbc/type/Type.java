/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.sink.SinkConnectorConfig;

/**
 * A type represents a relational column type used for query abd parameter binding.
 *
 * todo: this is heavily modeled after Hibernate's type system which perhaps could be used instead?
 * todo: is it possible to incorporate default value resolution into the type system?
 *
 * @author Chris Cranford
 */
public interface Type extends io.debezium.sink.type.Type {

    /**
     * Allows a type to perform initialization/configuration tasks based on user configs.
     *
     * @param config the JDBC sink connector's configuration, should not be {@code null}
     * @param dialect the database dialect, should not be {@code null}
     */
    void configure(SinkConnectorConfig config, DatabaseDialect dialect);

    /**
     * Return the SQL type name for this type.
     *
     * @param schema field schema, never {@code null}
     * @param isKey  whether the type resolution is for a key field
     * @return the resolved type to be used in DDL statements
     */
    String getTypeName(Schema schema, boolean isKey);
}
