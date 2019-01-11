/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import de.huxhorn.sulky.ulid.ULID;

import java.util.Optional;

public class UlidIdBuilder extends OrderedIdBuilder {
    private final ULID idGen = new ULID();
    private Clock clock = Clock.SYSTEM;

    protected ULID.Value lastId = new ULID.Value(0, 0);

    @Override
    public Boolean shouldIncludeId() {
        return true;
    }

    @Override
    public String buildNextId() {
        Optional<ULID.Value> n = next();
        while(!n.isPresent()) {
            // we just try and sleep here to next ms where
            // we likely won't get a problem
            // and ignore any interrupts, as we can just try
            // again on the next loop
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                // do nothing
            }
            n = next();
        }
        lastId = n.get();

        return lastId.toString();
    }

    @Override
    public String lastId() {
        return lastId.toString();
    }

    @Override
    public void restoreState(String state) {
        lastId = ULID.parseULID(state);
    }

    @Override
    public OrderedIdBuilder clone() {
        OrderedIdBuilder b = new UlidIdBuilder();
        b.restoreState(this.lastId());
        return b;
    }

    protected Optional<ULID.Value> next() {
        return idGen.nextStrictlyMonotonicValue(lastId, clock.currentTimeInMillis());
    }

    protected void setClock(Clock clock) {
        this.clock = clock;
    }
}
