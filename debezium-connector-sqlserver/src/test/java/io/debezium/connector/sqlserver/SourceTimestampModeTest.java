/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

import io.debezium.doc.FixFor;

public class SourceTimestampModeTest {

    @Test
    @FixFor("DBZ-1988")
    public void shouldConfigureDefaultMode() {
        assertThat(SourceTimestampMode.getDefaultMode()).isEqualTo(SourceTimestampMode.COMMIT);
    }

    @Test
    @FixFor("DBZ-1988")
    public void shouldReturnOptionFromValidMode() {
        assertThat(SourceTimestampMode.fromMode("processing")).isEqualTo(SourceTimestampMode.PROCESSING);
    }

    @Test
    @FixFor("DBZ-1988")
    public void shouldReturnDefaultIfGivenModeIsNull() {
        assertThat(SourceTimestampMode.fromMode(null)).isEqualTo(SourceTimestampMode.getDefaultMode());
    }
}
