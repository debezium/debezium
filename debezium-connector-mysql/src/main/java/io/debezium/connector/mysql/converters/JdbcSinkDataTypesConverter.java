/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.converters;

/**
 * MySQL handles several data types differently between streaming and snapshot and its important
 * that data types be handled consistently across both phases for JDBC sink connectors to create
 * the sink tables properly that adhere to the data provided in both phases.
 *
 * This converter specific makes the following changes:
 *      - {@code BOOLEAN} columns always emitted as INT16 schema types, true=1 and false=0.
 *      - {@code REAL} columns always emitted as FLOAT64 schema types.
 *      - String-based columns always emitted with "__debezium.source.column.character_set" parameter.
 *
 * @author Chris Cranford
 * @deprecated use {@link io.debezium.connector.binlog.converters.JdbcSinkDataTypesConverter} instead, remove in Debezium 3.0.
 */
@Deprecated
public class JdbcSinkDataTypesConverter extends io.debezium.connector.binlog.converters.JdbcSinkDataTypesConverter {

}
