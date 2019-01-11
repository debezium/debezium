/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.concurrent.atomic.AtomicLong;

public class CounterIdBuilder extends OrderedIdBuilder {
    private AtomicLong counter = new AtomicLong();
    @Override
    public Boolean shouldIncludeId() {
        return true;
    }

    @Override
    public String buildNextId() {
        return Long.toString(counter.incrementAndGet());
    }

    @Override
    public String lastId() {
        return Long.toString(counter.get());
    }

    @Override
    public void restoreState(String state) {
        counter = new AtomicLong(Long.parseLong(state));
    }

    @Override
    public OrderedIdBuilder clone() {
        OrderedIdBuilder b = new CounterIdBuilder();
        b.restoreState(this.lastId());
        return b;
    }
}
