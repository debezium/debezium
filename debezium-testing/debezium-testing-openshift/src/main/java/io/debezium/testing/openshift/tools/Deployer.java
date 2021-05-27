package io.debezium.testing.openshift.tools;

public interface Deployer<T> {

    interface Builder<D extends Deployer<?>> {
        D build();
    }

    /**
     * Deploys resource
     * @return Controller for deployed resource
     */
    T deploy() throws InterruptedException;
}
