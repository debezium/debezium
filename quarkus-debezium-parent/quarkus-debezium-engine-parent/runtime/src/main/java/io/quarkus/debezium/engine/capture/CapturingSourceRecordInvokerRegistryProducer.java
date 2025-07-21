/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.RecordChangeEvent;

public class CapturingSourceRecordInvokerRegistryProducer implements CapturingInvokerRegistryProducer<RecordChangeEvent<SourceRecord>> {
    private final Logger logger = LoggerFactory.getLogger(CapturingSourceRecordInvokerRegistryProducer.class);

    @Inject
    Instance<CapturingSourceRecordInvoker> invokers;

    @Override
    @Produces
    @Singleton
    public CapturingInvokerRegistry<RecordChangeEvent<SourceRecord>> produce() {
        CapturingInvoker<RecordChangeEvent<SourceRecord>> invoker = invokers
                .stream()
                .findFirst()
                .orElse(event -> logger.warn("no capture handler defined"));

        return identifier -> invoker;
    }
}
