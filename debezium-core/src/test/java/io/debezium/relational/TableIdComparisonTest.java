/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.fest.assertions.Assertions.assertThat;

public class TableIdComparisonTest {

    @Test
    public void testTableIdSelfCompare() {
        TableId left = new TableId("catalog1", "schema1", "table1");

        assertThat(left.compareTo(left)).isEqualTo(0);
    }

    @Test
    public void testTableIdCompare() {
        TableId left = new TableId("catalog1", "schema1", "table1");
        TableId right = new TableId("catalog1", "schema1", "table1");

        assertThat(left.compareTo(right)).isEqualTo(0);
    }

    @Test
    public void testTableIdCompareCaseSensitive() {
        TableId left = new TableId("catalog1", "schema1", "table1");
        TableId right = new TableId("CATALOG1", "schema1", "table1");

        assertThat(left.compareTo(right)).isGreaterThan(0);
        assertThat(right.compareTo(left)).isLessThan(0);
    }

    @Test
    public void testTableIdCompareIgnoreCase() {
        TableId left = new TableId("catalog1", "schema1", "table1");
        TableId right = new TableId("CATALOG1", "schema1", "table1");

        assertThat(left.compareToIgnoreCase(right)).isEqualTo(0);
    }

    @Test
    public void testTableIdCompareUnequal() {
        TableId left = new TableId("catalog1", "schema1", "table1");
        TableId right = new TableId("catalog1", "schema1", "table2");

        assertThat(left.compareTo(right)).isLessThan(0);
        assertThat(right.compareTo(left)).isGreaterThan(0);
    }

    @Test
    public void testTableIdCompareWithEmptyWhitelist() {
        List<String> whitelist = new ArrayList<>();
        TableId left = new TableId(null, "schema1", "table1");
        TableId right = new TableId(null, "schema1", "table2");

        assertThat(left.compareToWithWhitelist(right, whitelist)).isLessThan(0);
        assertThat(right.compareToWithWhitelist(left, whitelist)).isGreaterThan(0);
    }

    private List<String> getTestWhitelist() {
        List<String> whitelist = new ArrayList<>();
        whitelist.add("schema2.table1");
        whitelist.add("schema2.table2");
        whitelist.add("schema1.table1");
        whitelist.add("schema2.table3");
        whitelist.add("schema1.table2");

        return whitelist;
    }

    @Test
    public void testTableIdSelfCompareWithWhitelist() {
        TableId left = new TableId(null, "schema1", "table1");

        assertThat(left.compareToWithWhitelist(left, getTestWhitelist())).isEqualTo(0);
    }

    @Test
    public void testTableIdCompareWithWhitelistWithLeftNotPresent() {
        List<String> whitelist = new ArrayList<String>();
        TableId left = new TableId(null, "schema1", "table5");
        TableId right = new TableId(null, "schema1", "table2");

        assertThat(left.compareToWithWhitelist(right, getTestWhitelist())).isEqualTo(1);
    }

    @Test
    public void testTableIdCompareWithWhitelistWithRightNotPresent() {
        TableId left = new TableId(null, "schema1", "table1");
        TableId right = new TableId(null, "schema1", "table6");

        assertThat(left.compareToWithWhitelist(right, getTestWhitelist())).isEqualTo(-1);
    }

    @Test
    public void testTableIdCompareWithWhitelistWithNeitherPresent() {
        TableId left = new TableId(null, "schema1", "table5");
        TableId right = new TableId(null, "schema1", "table6");

        assertThat(left.compareToWithWhitelist(right, getTestWhitelist())).isEqualTo(left.compareToIgnoreCase(right));
    }

    @Test
    public void testTableIdCompareWithWhitelistWithSameTable() {
        TableId left = new TableId(null, "schema1", "table1");
        TableId right = new TableId(null, "schema1", "table1");

        assertThat(left.compareToWithWhitelist(right, getTestWhitelist())).isEqualTo(0);
    }

    @Test
    public void testTableIdCompareWithWhitelistWithLeftFirst() {
        TableId left = new TableId(null, "schema1", "table1");
        TableId right = new TableId(null, "schema1", "table2");

        assertThat(left.compareToWithWhitelist(right, getTestWhitelist())).isEqualTo(-1);
    }

    @Test
    public void testTableIdCompareWithWhitelistWithRightFirst() {
        TableId left = new TableId(null, "schema1", "table2");
        TableId right = new TableId(null, "schema1", "table1");

        assertThat(left.compareToWithWhitelist(right, getTestWhitelist())).isEqualTo(1);
    }
}
