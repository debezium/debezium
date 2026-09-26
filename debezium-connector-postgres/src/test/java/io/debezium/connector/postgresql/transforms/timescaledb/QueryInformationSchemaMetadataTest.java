/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.transforms.timescaledb;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.SocketException;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransientException;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

class QueryInformationSchemaMetadataTest {

    @Test
    void sqlTransientExceptionIsRetriable() {
        assertThat(QueryInformationSchemaMetadata.isRetriable(new SQLTransientException("temporary"))).isTrue();
    }

    @Test
    void sqlRecoverableExceptionIsRetriable() {
        assertThat(QueryInformationSchemaMetadata.isRetriable(new SQLRecoverableException("recoverable"))).isTrue();
    }

    @Test
    void socketResetWrappedInSqlExceptionIsRetriable() {
        var psqlException = new SQLException("An I/O error occurred while sending to the backend");
        psqlException.initCause(new SocketException("Connection reset by peer"));

        assertThat(QueryInformationSchemaMetadata.isRetriable(psqlException)).isTrue();
    }

    @Test
    void nestedSocketExceptionDeepInChainIsRetriable() {
        var cause = new SocketException("Broken pipe");
        var intermediate = new RuntimeException("wrapper", cause);
        var outer = new SQLException("outer", intermediate);

        assertThat(QueryInformationSchemaMetadata.isRetriable(outer)).isTrue();
    }

    @Test
    void sqlSyntaxErrorIsNotRetriable() {
        assertThat(QueryInformationSchemaMetadata.isRetriable(new SQLSyntaxErrorException("syntax error"))).isFalse();
    }

    @Test
    void plainExceptionWithoutTransientCauseIsNotRetriable() {
        assertThat(QueryInformationSchemaMetadata.isRetriable(new SQLException("some other failure"))).isFalse();
        assertThat(QueryInformationSchemaMetadata.isRetriable(new RuntimeException("unexpected", new IOException("disk full")))).isFalse();
    }

    @Test
    void nullThrowableIsNotRetriable() {
        assertThat(QueryInformationSchemaMetadata.isRetriable(null)).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2695")
    void hypertableIdIsParsedFromDefaultChunkName() {
        assertThat(QueryInformationSchemaMetadata.hypertableIdFromChunkName("_hyper_26_2403_chunk")).isEqualTo(OptionalInt.of(26));
    }

    @Test
    void hypertableIdIsNotParsedFromOtherTableNames() {
        assertThat(QueryInformationSchemaMetadata.hypertableIdFromChunkName("custom_chunk")).isEmpty();
        assertThat(QueryInformationSchemaMetadata.hypertableIdFromChunkName("compress_hyper_2_3_chunk")).isEmpty();
        assertThat(QueryInformationSchemaMetadata.hypertableIdFromChunkName("_hyper_x_1_chunk")).isEmpty();
    }
}
