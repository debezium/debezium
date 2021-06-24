/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.crdt;

import io.debezium.annotation.NotThreadSafe;

@NotThreadSafe
class StateBasedPNDeltaCounter extends StateBasedPNCounter implements DeltaCounter {
    private PNCounter delta;

    protected StateBasedPNDeltaCounter() {
        this(0L, 0L, 0L, 0L);
    }

    protected StateBasedPNDeltaCounter(long totalAdds, long totalRemoves, long recentAdds, long recentRemoves) {
        super(totalAdds, totalRemoves);
        delta = new StateBasedPNCounter(recentAdds, recentRemoves);
    }

    @Override
    public DeltaCounter increment() {
        super.increment();
        delta.increment();
        return this;
    }

    @Override
    public DeltaCounter decrement() {
        super.decrement();
        delta.decrement();
        return this;
    }

    @Override
    public long incrementAndGet() {
        delta.incrementAndGet();
        return super.incrementAndGet();
    }

    @Override
    public long decrementAndGet() {
        delta.decrementAndGet();
        return super.decrementAndGet();
    }

    @Override
    public long getAndIncrement() {
        delta.getAndIncrement();
        return super.getAndIncrement();
    }

    @Override
    public long getAndDecrement() {
        delta.getAndDecrement();
        return super.getAndDecrement();
    }

    @Override
    public PNCount getChanges() {
        return delta;
    }

    @Override
    public boolean hasChanges() {
        return delta.getIncrement() != 0 || delta.getDecrement() != 0;
    }

    @Override
    public Count getPriorCount() {
        long value = super.get() - delta.get();
        return new Count() {
            @Override
            public long get() {
                return value;
            }
        };
    }

    @Override
    public void reset() {
        delta = new StateBasedPNCounter();
    }

    @Override
    public DeltaCounter merge(Count other) {
        if (other instanceof DeltaCount) {
            // Just merge in the *changes* ...
            DeltaCount that = (DeltaCount) other;
            this.delta.merge(that.getChanges());
            super.merge(that.getChanges());
        }
        else {
            super.merge(other);
        }
        return this;
    }

    @Override
    public String toString() {
        return super.toString() + " (changes " + this.delta + ")";
    }
}
