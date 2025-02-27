/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.charset;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.charset.BinlogCharsetRegistry;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceRegistry;

/**
 * @author Chris Cranford
 */
public class MariaDbCharsetRegistryServiceProvider implements ServiceProvider<BinlogCharsetRegistry> {
    @Override
    public Class<BinlogCharsetRegistry> getServiceClass() {
        return BinlogCharsetRegistry.class;
    }

    @Override
    public BinlogCharsetRegistry createService(Configuration configuration, ServiceRegistry serviceRegistry) {
        return new MariaDbCharsetRegistry();
    }
}
