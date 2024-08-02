/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.debezium.connector.postgresql.data.vector.HalfVector;
import io.debezium.connector.postgresql.data.vector.SparseVector;
import io.debezium.connector.postgresql.data.vector.Vector;

public class VectorDatabaseTest {

    @Test
    public void shouldParseVector() {
        final var expectedVector = List.of(10.0, 20.0, 30.0);
        Assertions.assertThat(Vector.fromLogical(Vector.schema(), "[10,20,30]")).isEqualTo(expectedVector);
        Assertions.assertThat(Vector.fromLogical(Vector.schema(), "[ 10,20,30] ")).isEqualTo(expectedVector);
        Assertions.assertThat(Vector.fromLogical(Vector.schema(), " [ 10,20,30 ]")).isEqualTo(expectedVector);
        Assertions.assertThat(Vector.fromLogical(Vector.schema(), "[10 ,20 ,30]")).isEqualTo(expectedVector);
        Assertions.assertThat(Vector.fromLogical(Vector.schema(), "[10.2 , 20, 30]")).isEqualTo(List.of(10.2, 20.0, 30.0));
        Assertions.assertThat(Vector.fromLogical(Vector.schema(), "[10.2e-1 , 20, 30]")).isEqualTo(List.of(1.02, 20.0, 30.0));
    }

    @Test
    public void shouldIgnoreErrorInVectorFormat() {
        Assertions.assertThat(Vector.fromLogical(Vector.schema(), "10,20,30]")).isNull();
        Assertions.assertThat(Vector.fromLogical(Vector.schema(), "[10,20,30")).isNull();
        Assertions.assertThat(Vector.fromLogical(Vector.schema(), "{10,20,30}")).isNull();
    }

    @Test(expected = NumberFormatException.class)
    public void shouldFailOnNumberInVectorFormat() {
        Vector.fromLogical(Vector.schema(), "[a10,20,30]");
    }

    @Test
    public void shouldParseHalfVector() {
        final var expectedVector = List.of(10.0f, 20.0f, 30.0f);
        Assertions.assertThat(HalfVector.fromLogical(HalfVector.schema(), "[10,20,30]")).isEqualTo(expectedVector);
        Assertions.assertThat(HalfVector.fromLogical(HalfVector.schema(), "[ 10,20,30] ")).isEqualTo(expectedVector);
        Assertions.assertThat(HalfVector.fromLogical(HalfVector.schema(), " [ 10,20,30 ]")).isEqualTo(expectedVector);
        Assertions.assertThat(HalfVector.fromLogical(HalfVector.schema(), "[10 ,20 ,30]")).isEqualTo(expectedVector);
        Assertions.assertThat(HalfVector.fromLogical(HalfVector.schema(), "[10.2 , 20, 30]")).isEqualTo(List.of(10.2f, 20.0f, 30.0f));
        Assertions.assertThat(HalfVector.fromLogical(HalfVector.schema(), "[10.2e-1 , 20, 30]")).isEqualTo(List.of(1.02f, 20.0f, 30.0f));
    }

    @Test
    public void shouldIgnoreErrorInHalfVectorFormat() {
        Assertions.assertThat(HalfVector.fromLogical(HalfVector.schema(), "10,20,30]")).isNull();
        Assertions.assertThat(HalfVector.fromLogical(HalfVector.schema(), "[10,20,30")).isNull();
        Assertions.assertThat(HalfVector.fromLogical(HalfVector.schema(), "{10,20,30}")).isNull();
    }

    @Test(expected = NumberFormatException.class)
    public void shouldFailOnNumberInHalfVectorFormat() {
        HalfVector.fromLogical(HalfVector.schema(), "[a10,20,30]");
    }

    @Test
    public void shouldParseSparseVector() {
        final var expectedVector = Map.of((short) 1, 10.0, (short) 11, 20.0, (short) 111, 30.0);
        final var expectedDimensions = (short) 1000;

        var vector = SparseVector.fromLogical(SparseVector.schema(), "{1:10,11:20,111:30}/1000");
        Assertions.assertThat(vector.getInt16("dimensions")).isEqualTo(expectedDimensions);
        Assertions.assertThat(vector.getMap("vector")).isEqualTo(expectedVector);

        vector = SparseVector.fromLogical(SparseVector.schema(), "{1:10, 11:20, 111:30}/1000");
        Assertions.assertThat(vector.getInt16("dimensions")).isEqualTo(expectedDimensions);
        Assertions.assertThat(vector.getMap("vector")).isEqualTo(expectedVector);

        vector = SparseVector.fromLogical(SparseVector.schema(), " {1:10,11:20,111:30}/1000");
        Assertions.assertThat(vector.getInt16("dimensions")).isEqualTo(expectedDimensions);
        Assertions.assertThat(vector.getMap("vector")).isEqualTo(expectedVector);

        vector = SparseVector.fromLogical(SparseVector.schema(), "{1:10,11:20,111:30} /1000");
        Assertions.assertThat(vector.getInt16("dimensions")).isEqualTo(expectedDimensions);
        Assertions.assertThat(vector.getMap("vector")).isEqualTo(expectedVector);

        vector = SparseVector.fromLogical(SparseVector.schema(), "{1:10,11:20,111:30}/ 1000");
        Assertions.assertThat(vector.getInt16("dimensions")).isEqualTo(expectedDimensions);
        Assertions.assertThat(vector.getMap("vector")).isEqualTo(expectedVector);

        vector = SparseVector.fromLogical(SparseVector.schema(), "{1:10,11:20,111:30}/1000 ");
        Assertions.assertThat(vector.getInt16("dimensions")).isEqualTo(expectedDimensions);
        Assertions.assertThat(vector.getMap("vector")).isEqualTo(expectedVector);

        vector = SparseVector.fromLogical(SparseVector.schema(), "{1:10,11:20,111:30 }/1000");
        Assertions.assertThat(vector.getInt16("dimensions")).isEqualTo(expectedDimensions);
        Assertions.assertThat(vector.getMap("vector")).isEqualTo(expectedVector);

    }

    @Test
    public void shouldIgnoreErrorInSparseVectorFormat() {
        Assertions.assertThat(SparseVector.fromLogical(SparseVector.schema(), "{1:10,11:20,111:30}")).isNull();
        Assertions.assertThat(SparseVector.fromLogical(SparseVector.schema(), "{1:10,11:20,111:30/1000")).isNull();
        Assertions.assertThat(SparseVector.fromLogical(SparseVector.schema(), "1:10,11:20,111:30}/1000")).isNull();
        Assertions.assertThat(SparseVector.fromLogical(SparseVector.schema(), "{1:10,11:20,111:30}1000")).isNull();
        Assertions.assertThat(SparseVector.fromLogical(SparseVector.schema(), "/1000")).isNull();
        Assertions.assertThat(SparseVector.fromLogical(SparseVector.schema(), "{10,11:20,111:30}/1000")).isNull();
        Assertions.assertThat(SparseVector.fromLogical(SparseVector.schema(), "{1:10,11#20,111:30}/1000")).isNull();
    }

    @Test(expected = NumberFormatException.class)
    public void shouldFailOnNumberInSparseVectorFormat() {
        SparseVector.fromLogical(SparseVector.schema(), "{1:10,11:20,111:x}/1000");
    }
}
