/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools;

public interface Deployer<T> {

    /**
     * Deploys resource
     * @return Controller for deployed resource
     */
    T deploy() throws Exception;

    interface Builder<B extends Builder<B, D>, D extends Deployer<?>> {
        D build();

        @SuppressWarnings("unchecked")
        default B self() {
            return (B) this;
        }
    }
}
