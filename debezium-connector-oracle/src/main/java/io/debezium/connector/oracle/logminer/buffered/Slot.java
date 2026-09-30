/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

public abstract class Slot {
    int sqn = 0xffffffff;
    int index = -1;

    public boolean occupied() {
        return index > -1;
    }

    public void clear() {
    }
}
