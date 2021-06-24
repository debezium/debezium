/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.crdt;

import io.debezium.annotation.NotThreadSafe;

@NotThreadSafe
class StateBasedPNCounter implements PNCounter {
    private long adds;
    private long removes;

    protected StateBasedPNCounter() {
        this(0L, 0L);
    }

    protected StateBasedPNCounter(long adds, long removes) {
        this.adds = adds;
        this.removes = removes;
    }

    @Override
    public PNCounter increment() {
        ++adds;
        return this;
    }

    @Override
    public PNCounter decrement() {
        ++removes;
        return this;
    }

    @Override
    public long incrementAndGet() {
        return ++adds - removes;
    }

    @Override
    public long decrementAndGet() {
        return adds - ++removes;
    }

    @Override
    public long getAndIncrement() {
        return adds++ - removes;
    }

    @Override
    public long getAndDecrement() {
        return adds - removes++;
    }

    @Override
    public long get() {
        return adds - removes;
    }

    @Override
    public long getIncrement() {
        return adds;
    }

    @Override
    public long getDecrement() {
        return removes;
    }

    @Override
    public PNCounter merge(Count other) {
        if (other instanceof PNCount) {
            PNCount changes = (PNCount) other;
            this.adds += changes.getIncrement();
            this.removes += changes.getDecrement();
        }
        else if (other instanceof GCount) {
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
        return "+" + adds + " -" + removes;
    }
}
