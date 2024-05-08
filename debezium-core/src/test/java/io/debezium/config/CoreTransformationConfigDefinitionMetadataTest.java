/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import io.debezium.transforms.ByLogicalTableRouter;
import io.debezium.transforms.ExtractNewRecordState;
import io.debezium.transforms.outbox.EventRouter;
import io.debezium.transforms.tracing.ActivateTracingSpan;

/**
 * Applies basic checks of completeness of the SMT-specific configuration metadata for the Debezium core SMTs.
 */
public class CoreTransformationConfigDefinitionMetadataTest extends TransformationConfigDefinitionMetadataTest {

    public CoreTransformationConfigDefinitionMetadataTest() {
        super(
                new ByLogicalTableRouter<>(),
                new ExtractNewRecordState<>(),
                new EventRouter<>(),
                new ActivateTracingSpan<>());
    }
}
