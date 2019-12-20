/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.quarkus.outbox;

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;

/**
 * Quarkus deployment processor for the Debezium "outbox" extension.
 *
 * @author Chris Cranford
 */
public class OutboxProcessor {

    private static final String DEBEZIUM_OUTBOX = "debezium-outbox";
    private static final String DEBEZIUM_OUTBOX_CONFIG_PREFIX = "quarkus.debezium-outbox.";

    /**
     * Debezium Outbox configuration
     */
    DebeziumOutboxConfig debeziumOutboxConfig;

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(DEBEZIUM_OUTBOX);
    }

    @BuildStep
    void registerForReflection(BuildProducer<ReflectiveClassBuildItem> reflectiveClass) {
        reflectiveClass.produce(new ReflectiveClassBuildItem(true, true, true, ExportedEvent.class.getName()));
    }

    @BuildStep
    void build(BuildProducer<AdditionalBeanBuildItem> additionalBean) {
        additionalBean.produce(AdditionalBeanBuildItem.unremovableOf(EventDispatcher.class));
    }
}
