/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.reactive.quarkus.deployment;

import io.debezium.outbox.quarkus.deployment.DebeziumOutboxCommonConfig;
import io.debezium.outbox.quarkus.deployment.OutboxCommonProcessor;
import io.debezium.outbox.quarkus.deployment.OutboxEventEntityBuildItem;
import io.debezium.outbox.reactive.quarkus.internal.DebeziumOutboxHandler;
import io.debezium.outbox.reactive.quarkus.internal.DebeziumTracerEventDispatcher;
import io.debezium.outbox.reactive.quarkus.internal.DefaultEventDispatcher;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.Capabilities;
import io.quarkus.deployment.Capability;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.GeneratedResourceBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;

/**
 * Quarkus deployment processor for the Debezium "outbox" extension.
 *
 * @author Chris Cranford
 */
public final class OutboxProcessor extends OutboxCommonProcessor {

    /**
     * Debezium Outbox configuration
     */
    DebeziumOutboxConfig debeziumOutboxConfig;

    @Override
    protected DebeziumOutboxCommonConfig getConfig() {
        return debeziumOutboxConfig;
    }

    @BuildStep
    public void build(OutboxEventEntityBuildItem outboxBuildItem,
                      BuildProducer<AdditionalBeanBuildItem> additionalBeanProducer,
                      BuildProducer<GeneratedResourceBuildItem> generatedResourcesProducer,
                      BuildProducer<ReflectiveClassBuildItem> reflectiveClassProducer,
                      Capabilities capabilities) {
        if (debeziumOutboxConfig.tracingEnabled && capabilities.isPresent(Capability.OPENTRACING)) {
            additionalBeanProducer.produce(AdditionalBeanBuildItem.unremovableOf(DebeziumTracerEventDispatcher.class));

        }
        else {
            additionalBeanProducer.produce(AdditionalBeanBuildItem.unremovableOf(DefaultEventDispatcher.class));
        }
        additionalBeanProducer.produce(AdditionalBeanBuildItem.unremovableOf(DebeziumOutboxHandler.class));
        generateHbmMapping(outboxBuildItem, generatedResourcesProducer);
    }
}
