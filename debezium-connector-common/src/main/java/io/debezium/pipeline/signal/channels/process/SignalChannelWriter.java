/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.channels.process;

import io.debezium.pipeline.signal.SignalRecord;

/**
 * Interface for writing signals to a channel.
 */
public interface SignalChannelWriter {
    void signal(SignalRecord signal);
}
