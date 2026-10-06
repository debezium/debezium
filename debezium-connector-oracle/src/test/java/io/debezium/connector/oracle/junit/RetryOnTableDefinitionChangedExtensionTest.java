/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.junit;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.Optional;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link RetryOnTableDefinitionChangedExtension}.
 *
 * @author Chris Cranford
 */
public class RetryOnTableDefinitionChangedExtensionTest {

    @Test
    @FixFor("debezium/dbz#2787")
    public void shouldNotRetryWhenEngineDidNotFail() {
        assertThat(RetryOnTableDefinitionChangedExtension.isTableDefinitionChangedFailure(Optional.empty())).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2787")
    public void shouldNotRetryUnrelatedFailures() {
        final SQLException cause = new SQLException("ORA-00054: resource busy and acquire with NOWAIT specified", "61000", 54);
        final Throwable failure = new ConnectException("Snapshotting of table failed", cause);
        assertThat(RetryOnTableDefinitionChangedExtension.isTableDefinitionChangedFailure(Optional.of(failure))).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2787")
    public void shouldRetryWhenCauseChainContainsTableDefinitionChangedErrorCode() {
        final SQLException cause = new SQLException("unable to read data", "72000", 1466);
        final Throwable failure = new DebeziumException("Producer failure", new ConnectException("Snapshotting of table failed", cause));
        assertThat(RetryOnTableDefinitionChangedExtension.isTableDefinitionChangedFailure(Optional.of(failure))).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2787")
    public void shouldRetryWhenCauseChainMentionsTableDefinitionChangedErrorInMessage() {
        final Throwable failure = new DebeziumException("ORA-01466: unable to read data - table definition has changed");
        assertThat(RetryOnTableDefinitionChangedExtension.isTableDefinitionChangedFailure(Optional.of(failure))).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2787")
    public void shouldTerminateOnCyclicCauseChain() {
        final RuntimeException first = new RuntimeException("first");
        final RuntimeException second = new RuntimeException("second", first);
        first.initCause(second);
        assertThat(RetryOnTableDefinitionChangedExtension.isTableDefinitionChangedFailure(Optional.of(first))).isFalse();
    }
}
