/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.registry;

public interface RegistryController {

    /**
     * @return registry url
     */
    String getRegistryApiAddress();

    /**
     * @return registry public url
     */
    String getPublicRegistryApiAddress();

    /**
     * Waits for registry to be ready
     */
    void waitForRegistry() throws InterruptedException;

    /**
     * Undeploy this registry by deleting related ApicurioRegistry CR
     * @return true if the CR was found and deleted
     */
    boolean undeploy();
}
