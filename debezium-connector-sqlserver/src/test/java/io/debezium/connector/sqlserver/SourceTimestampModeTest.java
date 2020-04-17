package io.debezium.connector.sqlserver;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

public class SourceTimestampModeTest {

    @Test
    public void shouldConfigureDefaultMode() {
        assertThat(SourceTimestampMode.getDefaultMode()).isEqualTo(SourceTimestampMode.COMMIT);
    }

    @Test
    public void shouldReturnOptionFromValidMode() {
        assertThat(SourceTimestampMode.fromMode("processing")).isEqualTo(SourceTimestampMode.PROCESSING);
    }

    @Test
    public void shouldReturnDefaultIfGivenModeIsNull() {
        assertThat(SourceTimestampMode.fromMode(null)).isEqualTo(SourceTimestampMode.getDefaultMode());
    }
}
