/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service.spi;

import io.debezium.common.annotation.Incubating;

/**
 * Contract for supplying custom Debezium {@link io.debezium.service.Service services}. Contributors
 * are applied when the service registry is created, and a contributed provider takes precedence over
 * Debezium's default provider for the same service, including the defaults a connector task registers
 * later.
 *
 * @author Chris Cranford
 */
@Incubating
public interface ServiceProviderContributor {
    /**
     * Contribute a custom service to the {@link ServiceRegistry}.
     *
     * @param registrar the registrar for contributed service providers, never {@code null}
     */
    void contribute(ServiceProviderRegistrar registrar);
}
