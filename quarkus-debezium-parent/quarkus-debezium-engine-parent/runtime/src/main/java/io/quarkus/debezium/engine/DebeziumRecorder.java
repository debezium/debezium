/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.util.concurrent.ExecutorService;

import io.debezium.runtime.Debezium;
import io.quarkus.arc.runtime.BeanContainer;
import io.quarkus.runtime.ShutdownContext;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class DebeziumRecorder {

    public void startEngine(ExecutorService executorService, ShutdownContext context, BeanContainer container) {
        DebeziumRunner runner = new DebeziumRunner(executorService, container.beanInstance(Debezium.class));

        runner.start();
        context.addShutdownTask(runner::shutdown);
    }
}
