/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.RecordChangeEvent;
import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvent;

@ApplicationScoped
public class CaptureHandler {

    private final CaptureService captureService;
    private final Logger logger = LoggerFactory.getLogger(CaptureHandler.class);

    public CaptureHandler(CaptureService captureService) {
        this.captureService = captureService;
    }

    @Capturing
    public void capture(RecordChangeEvent<SourceRecord> event) {
        captureService.capture();
    }

    @Capturing(destination = "dbserver1.public.products")
    public void products(CapturingEvent<Product> event) {
        logger.info("getting an event from {}", event.destination());
        captureService.add(event.record());
    }
}
