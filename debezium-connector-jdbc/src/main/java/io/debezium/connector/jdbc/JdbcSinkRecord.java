/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Struct;

import io.debezium.annotation.Immutable;
import io.debezium.bindings.kafka.DebeziumSinkRecord;
import io.debezium.connector.jdbc.field.JdbcFieldDescriptor;

/**
 * @author rk3rn3r
 */
@Immutable
public interface JdbcSinkRecord extends DebeziumSinkRecord {

    Map<String, JdbcFieldDescriptor> jdbcFields();

    Set<String> keyFieldNames();

    Set<String> nonKeyFieldNames();

    Struct filteredKey();

}
