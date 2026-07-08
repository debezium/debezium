/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolver;

/**
 * A Hibernate {@link DialectResolver} that resolves the {@link StarRocksDialect} when the JDBC
 * driver reports the {@code StarRocks} database product name, as the StarRocks JDBC driver
 * ({@code com.starrocks:starrocks-connector-j}) does. This enables automatic dialect detection
 * without requiring the {@code hibernate.dialect} property to be supplied. The resolver is
 * registered through the {@code hibernate.dialect_resolvers} setting by
 * {@link io.debezium.connector.jdbc.JdbcSinkConnectorConfig}.
 */
public class StarRocksDialectResolver implements DialectResolver {

    private static final String STARROCKS_PRODUCT_NAME = "StarRocks";

    @Override
    public Dialect resolveDialect(DialectResolutionInfo info) {
        if (STARROCKS_PRODUCT_NAME.equalsIgnoreCase(info.getDatabaseName())) {
            return new StarRocksDialect(info);
        }
        return null;
    }
}
