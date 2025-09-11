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

import io.quarkus.debezium.notification.SnapshotEvent;

@ApplicationScoped
public class SnapshotEventObserver {

    private final List<SnapshotEvent> snapshotEvents = new ArrayList<>();

    public void snapshot(@Observes SnapshotEvent snapshot) {
        snapshotEvents.add(snapshot);
    }

    public List<SnapshotEvent> getSnapshotEvents() {
        return snapshotEvents;
    }
}
