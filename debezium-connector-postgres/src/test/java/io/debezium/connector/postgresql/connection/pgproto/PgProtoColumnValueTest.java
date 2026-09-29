/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgproto;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;

import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.proto.PgProto;
import io.debezium.doc.FixFor;

class PgProtoColumnValueTest {

    /**
     * Days between the PostgreSQL epoch (2000-01-01) and the Unix epoch, as added by decoderbufs.
     */
    private static final int POSTGRES_EPOCH_OFFSET_DAYS = 10957;

    @Test
    @FixFor("debezium/dbz#2721")
    void shouldMapWrappedInfinityDatesToConnectorSentinels() {
        // decoderbufs adds the epoch offset to DATEVAL_NOEND/DATEVAL_NOBEGIN without an infinity check
        assertThat(dateOf(Integer.MAX_VALUE + POSTGRES_EPOCH_OFFSET_DAYS)).isEqualTo(PostgresValueConverter.POSITIVE_INFINITY_LOCAL_DATE);
        assertThat(dateOf(Integer.MIN_VALUE + POSTGRES_EPOCH_OFFSET_DAYS)).isEqualTo(PostgresValueConverter.NEGATIVE_INFINITY_LOCAL_DATE);
    }

    @Test
    @FixFor("debezium/dbz#2721")
    void shouldReadFiniteDatesAsEpochDays() {
        assertThat(dateOf(0)).isEqualTo(LocalDate.EPOCH);
        assertThat(dateOf((int) LocalDate.of(0, 3, 7).toEpochDay())).isEqualTo(LocalDate.of(0, 3, 7));
        assertThat(dateOf((int) LocalDate.of(5874897, 12, 31).toEpochDay())).isEqualTo(LocalDate.of(5874897, 12, 31));
    }

    private static LocalDate dateOf(int epochDay) {
        return new PgProtoColumnValue(PgProto.DatumMessage.newBuilder().setDatumInt32(epochDay).build()).asLocalDate();
    }
}
