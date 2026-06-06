/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.binlog.BinlogZeroDateFallbackConverterIT;

/**
 * MySQL-specific tests for the {@link io.debezium.connector.binlog.converters.ZeroDateFallbackConverter}.
 *
 * @author minleejae
 */
public class MySqlZeroDateFallbackConverterIT extends BinlogZeroDateFallbackConverterIT<MySqlConnector> implements MySqlCommon {

}
