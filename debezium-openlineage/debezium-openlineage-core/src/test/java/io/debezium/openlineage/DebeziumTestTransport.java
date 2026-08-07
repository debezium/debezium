/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.openlineage.client.OpenLineage.DatasetEvent;
import io.openlineage.client.OpenLineage.JobEvent;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.transports.Transport;

public class DebeziumTestTransport extends Transport {

    private final List<RunEvent> runEvents = new CopyOnWriteArrayList<>();
    private final List<DatasetEvent> datasetEvents = new CopyOnWriteArrayList<>();
    private final List<JobEvent> jobEvents = new CopyOnWriteArrayList<>();

    @Override
    public void emit(RunEvent runEvent) {
        runEvents.add(runEvent);
    }

    @Override
    public void emit(DatasetEvent datasetEvent) {
        datasetEvents.add(datasetEvent);
    }

    @Override
    public void emit(JobEvent jobEvent) {
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

    public void clear() {
        runEvents.clear();
        datasetEvents.clear();
        jobEvents.clear();
    }
}
