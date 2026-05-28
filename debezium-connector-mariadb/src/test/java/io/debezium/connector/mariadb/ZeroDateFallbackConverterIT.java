/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.connector.binlog.BinlogZeroDateFallbackConverterIT;

/**
 * MariaDB-specific tests for the {@link io.debezium.connector.binlog.converters.ZeroDateFallbackConverter}.
 *
 * @author minleejae
 */
public class ZeroDateFallbackConverterIT extends BinlogZeroDateFallbackConverterIT<MariaDbConnector> implements MariaDbCommon {

}
