/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.engine.RecordChangeEvent;

class DefaultFullyQualifiedTableNameResolver implements FullyQualifiedTableNameResolver {

    @Override
    public String resolve(RecordChangeEvent<SourceRecord> event) {
        Struct source = ((Struct) event.record().value()).getStruct("source");

        return source.getString("schema") + "." + source.getString("table");
    }
}
