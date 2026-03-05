/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.service;

import io.debezium.connector.mariadb.charset.MariaDbCharsetRegistryServiceProvider;
import io.debezium.service.spi.ServiceProviderContributor;
import io.debezium.service.spi.ServiceRegistryBuilder;

/**
 * Contributes custom services for the MariaDB connector.
 *
 * @author Chris Cranford
 */
public class MariaDbServiceProviderContributor implements ServiceProviderContributor {
    @Override
    public void contribute(ServiceRegistryBuilder registryBuilder) {
        registryBuilder.registerServiceProvider(new MariaDbCharsetRegistryServiceProvider());
    }
}
