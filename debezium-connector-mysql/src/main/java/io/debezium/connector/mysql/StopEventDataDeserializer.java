/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.io.IOException;

import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

/**
 * @author Randall Hauch
 */
public class StopEventDataDeserializer implements EventDataDeserializer<StopEventData> {

    @Override
    public StopEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        return new StopEventData();
    }
}
