/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.data.vector;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class VectorDatatypeTest {

    @Test
    public void shouldParseVector() {
        final var expectedVector = List.of(10.0, 20.0, 30.0);
        Assertions.assertThat(DoubleVector.fromLogical(DoubleVector.schema(), "[10,20,30]")).isEqualTo(expectedVector);
        Assertions.assertThat(DoubleVector.fromLogical(DoubleVector.schema(), "[ 10,20,30] ")).isEqualTo(expectedVector);
        Assertions.assertThat(DoubleVector.fromLogical(DoubleVector.schema(), " [ 10,20,30 ]")).isEqualTo(expectedVector);
        Assertions.assertThat(DoubleVector.fromLogical(DoubleVector.schema(), "[10 ,20 ,30]")).isEqualTo(expectedVector);
        Assertions.assertThat(DoubleVector.fromLogical(DoubleVector.schema(), "[10.2 , 20, 30]")).isEqualTo(List.of(10.2, 20.0, 30.0));
        Assertions.assertThat(DoubleVector.fromLogical(DoubleVector.schema(), "[10.2e-1 , 20, 30]")).isEqualTo(List.of(1.02, 20.0, 30.0));
    }

    @Test
    public void shouldIgnoreErrorInVectorFormat() {
        Assertions.assertThat(DoubleVector.fromLogical(DoubleVector.schema(), "10,20,30]")).isNull();
        Assertions.assertThat(DoubleVector.fromLogical(DoubleVector.schema(), "[10,20,30")).isNull();
        Assertions.assertThat(DoubleVector.fromLogical(DoubleVector.schema(), "{10,20,30}")).isNull();
    }

    @Test(expected = NumberFormatException.class)
    public void shouldFailOnNumberInVectorFormat() {
        DoubleVector.fromLogical(DoubleVector.schema(), "[a10,20,30]");
    }

    @Test
    public void shouldParseHalfVector() {
        final var expectedVector = List.of(10.0f, 20.0f, 30.0f);
        Assertions.assertThat(FloatVector.fromLogical(FloatVector.schema(), "[10,20,30]")).isEqualTo(expectedVector);
        Assertions.assertThat(FloatVector.fromLogical(FloatVector.schema(), "[ 10,20,30] ")).isEqualTo(expectedVector);
        Assertions.assertThat(FloatVector.fromLogical(FloatVector.schema(), " [ 10,20,30 ]")).isEqualTo(expectedVector);
        Assertions.assertThat(FloatVector.fromLogical(FloatVector.schema(), "[10 ,20 ,30]")).isEqualTo(expectedVector);
        Assertions.assertThat(FloatVector.fromLogical(FloatVector.schema(), "[10.2 , 20, 30]")).isEqualTo(List.of(10.2f, 20.0f, 30.0f));
        Assertions.assertThat(FloatVector.fromLogical(FloatVector.schema(), "[10.2e-1 , 20, 30]")).isEqualTo(List.of(1.02f, 20.0f, 30.0f));
    }

    @Test
    public void shouldIgnoreErrorInHalfVectorFormat() {
        Assertions.assertThat(FloatVector.fromLogical(FloatVector.schema(), "10,20,30]")).isNull();
        Assertions.assertThat(FloatVector.fromLogical(FloatVector.schema(), "[10,20,30")).isNull();
        Assertions.assertThat(FloatVector.fromLogical(FloatVector.schema(), "{10,20,30}")).isNull();
    }

    @Test(expected = NumberFormatException.class)
    public void shouldFailOnNumberInHalfVectorFormat() {
        FloatVector.fromLogical(FloatVector.schema(), "[a10,20,30]");
    }
}
