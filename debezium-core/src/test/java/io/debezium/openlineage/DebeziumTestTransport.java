/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import java.util.ArrayList;
import java.util.List;

import io.openlineage.client.OpenLineage.DatasetEvent;
import io.openlineage.client.OpenLineage.JobEvent;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.transports.Transport;

import lombok.NonNull;

public class DebeziumTestTransport extends Transport {

    private final List<RunEvent> runEvents = new ArrayList<>();
    private final List<DatasetEvent> datasetEvents = new ArrayList<>();
    private final List<JobEvent> jobEvents = new ArrayList<>();

    @Override
    public void emit(@NonNull RunEvent runEvent) {
        System.out.println("ADDED RUN EVENT");
        runEvents.add(runEvent);
    }

    @Override
    public void emit(@NonNull DatasetEvent datasetEvent) {
        System.out.println("ADDED DATASET EVENT");
        datasetEvents.add(datasetEvent);
    }

    @Override
    public void emit(@NonNull JobEvent jobEvent) {
        System.out.println("ADDED JOB EVENT");
        jobEvents.add(jobEvent);
    }

    public List<RunEvent> getRunEvents() {
        return runEvents;
    }

    public List<DatasetEvent> getDatasetEvents() {
        return datasetEvents;
    }

    public List<JobEvent> getJobEvents() {
        return jobEvents;
    }
}
