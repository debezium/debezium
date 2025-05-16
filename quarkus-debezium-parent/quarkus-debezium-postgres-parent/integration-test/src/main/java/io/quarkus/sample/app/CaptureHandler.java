/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.engine.RecordChangeEvent;
import io.debezium.runtime.Capturing;

@ApplicationScoped
public class CaptureHandler {

    private final CaptureService captureService;

    public CaptureHandler(CaptureService captureService) {
        this.captureService = captureService;
    }

    @Capturing
    public void capture(RecordChangeEvent<SourceRecord> event) {
        captureService.capture();
    }
}
