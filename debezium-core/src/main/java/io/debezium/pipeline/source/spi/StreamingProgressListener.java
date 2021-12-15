/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

/**
 * Invoked whenever an important event or change of state happens during the streaming phase.
 */
public interface StreamingProgressListener {

    void connected(boolean connected);
}
