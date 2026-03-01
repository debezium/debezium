/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.service;

import io.debezium.connector.mysql.charset.MySqlCharsetRegistryServiceProvider;
import io.debezium.service.spi.ServiceProviderContributor;
import io.debezium.service.spi.ServiceRegistryBuilder;

/**
 * Contributes custom services for the MySQL connector.
 *
 * @author Chris Cranford
 */
public class MySqlServiceProviderContributor implements ServiceProviderContributor {
    @Override
    public void contribute(ServiceRegistryBuilder registryBuilder) {
        registryBuilder.registerServiceProvider(new MySqlCharsetRegistryServiceProvider());
    }
}
