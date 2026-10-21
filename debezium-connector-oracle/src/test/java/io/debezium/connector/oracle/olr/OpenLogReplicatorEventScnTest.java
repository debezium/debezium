/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.olr.client.StreamingEvent;
import io.debezium.doc.FixFor;

/**
 * Tests which SCN the OpenLogReplicator adapter reports as {@code source.scn}.
 */
public class OpenLogReplicatorEventScnTest {

    private final ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    @Test
    @FixFor("debezium/dbz#2912")
    public void shouldUseTheRowScnWhenTheMessageHasOne() throws Exception {
        final StreamingEvent event = read("{\"scn\":100,\"c_scn\":120,\"c_idx\":1,\"xid\":\"0x0001.002.00000003\",\"payload\":[]}");
        assertThat(OpenLogReplicatorStreamingChangeEventSource.getEventScn(event)).isEqualTo(Scn.valueOf(100));
    }

    @Test
    @FixFor("debezium/dbz#2912")
    public void shouldUseCheckpointScnWhenTheMessageHasNoRowScn() throws Exception {
        final StreamingEvent event = read("{\"c_scn\":120,\"c_idx\":1,\"xid\":\"0x0001.002.00000003\",\"payload\":[]}");
        assertThat(OpenLogReplicatorStreamingChangeEventSource.getEventScn(event)).isEqualTo(Scn.valueOf(120));
    }

    private StreamingEvent read(String json) throws Exception {
        return mapper.readValue(json, StreamingEvent.class);
    }
}
