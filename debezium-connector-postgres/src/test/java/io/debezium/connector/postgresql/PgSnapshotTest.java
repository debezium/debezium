/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.debezium.DebeziumException;

public class PgSnapshotTest {

    @Test
    public void parseCorrectPgSnapshotWithInProgressTransactions() {

        PgSnapshot snapshot = PgSnapshot.valueOf("795:799:795,797");

        Assertions.assertThat(snapshot.getXMin()).isEqualTo(795L);
        Assertions.assertThat(snapshot.getXMax()).isEqualTo(799L);
        Assertions.assertThat(snapshot.getXip()).contains(795L, 797L);
    }

    @Test
    public void parseCorrectPgSnapshotWithoutInProgressTransactions() {

        PgSnapshot snapshot = PgSnapshot.valueOf("795:799:");

        Assertions.assertThat(snapshot.getXMin()).isEqualTo(795L);
        Assertions.assertThat(snapshot.getXMax()).isEqualTo(799L);
        Assertions.assertThat(snapshot.getXip()).isEmpty();
    }

    @Test
    public void parseAWrongPgSnapshotWillThrowException() {

        Assertions.assertThatThrownBy(() -> PgSnapshot.valueOf("795::"))
                .isInstanceOf(DebeziumException.class);
    }
}
