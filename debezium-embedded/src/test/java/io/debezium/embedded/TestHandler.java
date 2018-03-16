/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHandler implements ChangeEventHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TestHandler.class);

    private int counter = 0;

    @Override
    public void handle(SourceRecord record) {
        LOG.info("Message arrived {}", record);
        if (++counter == 5) {
            throw new StopConnectorException("Completed");
        }
    }

}
