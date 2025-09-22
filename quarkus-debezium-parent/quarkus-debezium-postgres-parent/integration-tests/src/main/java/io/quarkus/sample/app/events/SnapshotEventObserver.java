/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app.events;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

import io.debezium.runtime.events.DefaultEngine;
import io.debezium.runtime.events.Engine;
import io.quarkus.debezium.notification.SnapshotEvent;

@ApplicationScoped
public class SnapshotEventObserver {

    private final List<SnapshotEvent> defaultSnapshotEvents = new ArrayList<>();
    private final List<SnapshotEvent> alternativeSnapshotEvents = new ArrayList<>();

    public void defaultSnapshot(@Observes @DefaultEngine SnapshotEvent snapshot) {
        defaultSnapshotEvents.add(snapshot);
    }

    public void alternativeSnapshot(@Observes @Engine("alternative") SnapshotEvent snapshot) {
        alternativeSnapshotEvents.add(snapshot);
    }

    public List<SnapshotEvent> getDefaultSnapshotEvents(String engine) {
        if (engine.equals("default")) {
            return defaultSnapshotEvents;
        }
        return alternativeSnapshotEvents;
    }

}
