/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class Batch extends ArrayList<BatchRecord> {

    public Batch(Collection<? extends BatchRecord> collection) {
        super(Collections.unmodifiableList(new ArrayList<>(collection)));
    }

}
