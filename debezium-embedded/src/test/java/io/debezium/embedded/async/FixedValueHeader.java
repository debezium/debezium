/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;

public class FixedValueHeader<R extends ConnectRecord<R>> implements Transformation<R> {

    @Override
    public R apply(R record) {
        Headers headers = new ConnectHeaders();
        headers.add("fixed-key", 2, Schema.INT32_SCHEMA);
        headers.forEach(h -> record.headers().add(h));

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp(),
                headers);
    }

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void close() {
    }

}
