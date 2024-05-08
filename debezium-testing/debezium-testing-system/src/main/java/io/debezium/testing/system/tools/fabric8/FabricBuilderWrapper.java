/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.fabric8;

import java.util.function.Consumer;

/**
 * Wraps Strimzi builder in order to provide convenience methods for Debezium configuration
 *
 * @param <I> type of the child wrapper instance
 * @param <B> type of Fabric8 builder
 * @param <R> type of resource build by Faric8 builder
 */
public abstract class FabricBuilderWrapper<I extends FabricBuilderWrapper<I, B, R>, B, R> {

    protected B builder;

    protected FabricBuilderWrapper(B builder) {
        this.builder = builder;
    }

    @SuppressWarnings("unchecked")
    protected I self() {
        return (I) this;
    }

    protected I onBuilder(Consumer<B> configurator) {
        configurator.accept(builder);
        return self();
    }

    public abstract R build();
}
