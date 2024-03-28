/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.event;

import java.io.IOException;

import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

/**
 * A deserializer that decodes {@link StopEventData}.
 *
 * @author Randall Hauch
 * @author Chris Cranford
 */
public class StopEventDataDeserializer implements EventDataDeserializer<StopEventData> {
    @Override
    public StopEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        return new StopEventData();
    }
}
