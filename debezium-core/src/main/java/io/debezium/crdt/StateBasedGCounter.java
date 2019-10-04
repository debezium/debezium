/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.crdt;

import io.debezium.annotation.NotThreadSafe;

@NotThreadSafe
class StateBasedGCounter implements GCounter {
    private long adds;

    protected StateBasedGCounter() {
        this(0L);
    }

    protected StateBasedGCounter(long adds) {
        this.adds = adds;
    }

    @Override
    public GCounter increment() {
        ++adds;
        return this;
    }

    @Override
    public long incrementAndGet() {
        return ++adds;
    }

    @Override
    public long getAndIncrement() {
        return adds++;
    }

    @Override
    public long get() {
        return adds;
    }

    @Override
    public long getIncrement() {
        return adds;
    }

    @Override
    public GCounter merge(Count other) {
        if (other instanceof GCount) {
            GCount changes = (GCount) other;
            this.adds += changes.getIncrement();
        }
        else if (other instanceof Count) {
            Count changes = other;
            this.adds += changes.get();
        }
        return this;
    }

    @Override
    public String toString() {
        return "+" + adds;
    }
}
