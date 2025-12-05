/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.text;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

/**
 * @author Randall Hauch
 */
public class PositionTest {

    private int combinedIndex(int firstIndex,
                              int secondIndex) {
        Position first = new Position(firstIndex, 1, 0);
        Position second = new Position(secondIndex, 1, 0);
        int firstPlusSecond = first.add(second).index();
        int secondPlusFirst = second.add(first).index();
        assertThat(firstPlusSecond).isEqualTo(secondPlusFirst);
        return firstPlusSecond;
    }

    @Test
    public void shouldAddNoContentPositionToValidPosition() {
        // -1 to >=0
        assertThat(combinedIndex(-1, 0)).isEqualTo(0);
        assertThat(combinedIndex(-1, 1)).isEqualTo(1);
        assertThat(combinedIndex(-1, 10)).isEqualTo(10);
    }

    @Test
    public void shouldAddValidPositionToNoContentPosition() {
        // >= 0 to -1
        assertThat(combinedIndex(0, -1)).isEqualTo(0);
        assertThat(combinedIndex(1, -1)).isEqualTo(1);
        assertThat(combinedIndex(10, -1)).isEqualTo(10);
    }

    @Test
    public void shouldAddValidPositionToValidPosition() {
        // positive to positive
        assertThat(combinedIndex(1, 1)).isEqualTo(2);
        assertThat(combinedIndex(10, 1)).isEqualTo(11);
        assertThat(combinedIndex(1, 10)).isEqualTo(11);
        assertThat(combinedIndex(10, 10)).isEqualTo(20);
    }

    @Test
    public void shouldAddStartingPositionToStartingPosition() {
        // 0 to 0
        assertThat(combinedIndex(0, 0)).isEqualTo(0);
    }

    @Test
    public void shouldAddNoContentPositionToNoContentPosition() {
        // -1 to -1
        assertThat(combinedIndex(-1, -1)).isEqualTo(-1);
        assertThat(combinedIndex(-10, -1)).isEqualTo(-1);
        assertThat(combinedIndex(-1, -10)).isEqualTo(-1);
    }
}
