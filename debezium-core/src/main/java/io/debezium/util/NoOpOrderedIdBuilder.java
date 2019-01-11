/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

public class NoOpOrderedIdBuilder extends OrderedIdBuilder {

    @Override
    public Boolean shouldIncludeId() {
        return false;
    }

    @Override
    public String buildNextId() {
        return null;
    }

    @Override
    public String lastId() {
        return null;
    }

    @Override
    public void restoreState(String state) {

    }

    @Override
    public OrderedIdBuilder clone() {
        return new NoOpOrderedIdBuilder();
    }
}
