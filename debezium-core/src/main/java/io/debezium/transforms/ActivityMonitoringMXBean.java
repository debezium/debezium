/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.util.Map;

/**
 * Exposes advanced metrics used for monitoring DB activity.
 */
public interface ActivityMonitoringMXBean {

    Map<String, Long> getNumberOfCreateEventsSeen();

    Map<String, Long> getNumberOfDeleteEventsSeen();

    Map<String, Long> getNumberOfUpdateEventsSeen();

    Map<String, Long> getNumberOfTruncateEventsSeen();

    void pause();

    void resume();
}
