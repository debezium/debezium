/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.doc.FixFor;

/**
 * @author Chris Cranford
 */
public class RsIdCodecTest {

    @ParameterizedTest
    @ValueSource(strings = { "0x000013.00001a2b.0010", "0x000000.00000000.0000", "0xffffff.ffffffff.ffff", "0xabc123.0fedcba9.8000" })
    @FixFor("debezium/dbz#2935")
    public void shouldRoundTripPackedForm(String rsId) {
        final RsIdCodec.Packed packed = RsIdCodec.encode(rsId);
        assertThat(packed).isNotNull();
        assertThat(RsIdCodec.decode(packed.blockSequence(), packed.offset())).isEqualTo(rsId);
    }

    @Test
    @FixFor("debezium/dbz#2935")
    public void shouldPackSegmentsIntoTheirBits() {
        final RsIdCodec.Packed packed = RsIdCodec.encode("0x000013.00001a2b.0010");
        assertThat(packed.blockSequence()).isEqualTo((0x13L << 32) | 0x1a2bL);
        assertThat(packed.offset()).isEqualTo((short) 0x10);
    }

    @ParameterizedTest
    @FixFor("debezium/dbz#2935")
    @NullSource
    @ValueSource(strings = { "", "1", " 0x000013.00001a2b.0010", "0x000013.00001A2B.0010", "0x00013.00001a2b.0010", "0x000013-00001a2b-0010",
            "0x000013.00001a2b.00100", "0X000013.00001a2b.0010" })
    public void shouldNotPackOtherForms(String rsId) {
        assertThat(RsIdCodec.encode(rsId)).isNull();
    }
}
