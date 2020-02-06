/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.pipeline;

import java.util.Map;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.CdcSourceTaskContext;

public interface ChangeEventSourceFacade<T extends CdcSourceTaskContext> {

    void start();

    void commitOffset(Map<String, ?> offset);

    void stop();

    ChangeEventQueue<DataChangeEvent> getQueue();

    ChangeEventSourceCoordinator getCoordinator();
}
