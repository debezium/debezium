/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.scripting;

import org.apache.kafka.connect.data.Schema;

public class RecordHeader {

    /**
     * Value of the header
     */
    public final Object value;

    /**
     * Schema of the header
     */
    public final Schema schema;

    public RecordHeader(Schema schema, Object value) {
        super();
        this.value = value;
        this.schema = schema;
    }

    @Override
    public String toString() {
        return "RecordHeader [value=" + value + ", schema=" + schema + "]";
    }
}
