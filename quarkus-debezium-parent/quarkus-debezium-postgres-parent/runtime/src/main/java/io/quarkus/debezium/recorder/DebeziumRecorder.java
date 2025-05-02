/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.recorder;

import java.util.concurrent.ExecutorService;

import io.quarkus.arc.Arc;
import io.quarkus.debezium.engine.Debezium;
import io.quarkus.debezium.engine.DebeziumRunner;
import io.quarkus.runtime.ShutdownContext;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class DebeziumRecorder {

    public void startEngine(ExecutorService executorService, ShutdownContext context) {
        try (var instance = Arc.container().instance(Debezium.class)) {
            Debezium debezium = instance.get();

            DebeziumRunner runner = new DebeziumRunner(executorService, debezium);
            runner.start();

            context.addShutdownTask(runner::shutdown);
        }
    }
}
