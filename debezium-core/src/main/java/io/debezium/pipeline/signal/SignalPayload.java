/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import java.util.Map;

import io.debezium.document.Document;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;

public class SignalPayload<P extends Partition> {

    public final String id;
    public final String type;
    public final Document data;
    public final P partition;
    public final OffsetContext offsetContext;
    public final Map<String, Object> additionalData;

    /**
     * @param partition     partition from which the signal was sent
     * @param id            identifier of the signal intended for deduplication, usually ignored by the signal
     * @param type          of the signal, usually ignored by the signal, should be used only when a signal code is shared for multiple signals
     * @param data          data specific for given signal instance
     * @param offsetContext offset at what the signal was sent
     * @param additionalData additional data specific to the channel
     */
    public SignalPayload(P partition, String id, String type, Document data, OffsetContext offsetContext, Map<String, Object> additionalData) {
        super();
        this.partition = partition;
        this.id = id;
        this.type = type;
        this.data = data;
        this.offsetContext = offsetContext;
        this.additionalData = additionalData;
    }

    @Override
    public String toString() {
        return "SignalPayload{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", data=" + data +
                ", partition=" + partition +
                ", offsetContext=" + offsetContext +
                ", additionalData=" + additionalData +
                '}';
    }
}
