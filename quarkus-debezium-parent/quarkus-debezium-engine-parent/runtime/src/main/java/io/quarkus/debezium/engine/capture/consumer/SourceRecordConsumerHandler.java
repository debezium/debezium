/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture.consumer;

import io.debezium.runtime.EngineManifest;

public interface SourceRecordConsumerHandler {
    SourceRecordEventConsumer get(EngineManifest manifest);
}
