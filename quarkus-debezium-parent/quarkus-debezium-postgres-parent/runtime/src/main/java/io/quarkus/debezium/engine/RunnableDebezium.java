/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.engine;

import java.io.IOException;

public abstract class RunnableDebezium implements Debezium {
    protected abstract void run();

    protected abstract void close() throws IOException;
}
