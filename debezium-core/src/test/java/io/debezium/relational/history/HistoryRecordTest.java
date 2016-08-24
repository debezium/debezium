/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.util.Collect;

/**
 * @author Randall Hauch
 *
 */
public class HistoryRecordTest {
    
    private Map<String,Object> source1;
    private Map<String,Object> position1;
    private HistoryRecord record1;
    private Map<String,Object> source2;
    private Map<String,Object> position2;
    private HistoryRecord record2;
    private Map<String,Object> source3;
    private Map<String,Object> position3;
    private HistoryRecord record3;

    @Before
    public void beforeEach() {
        source1 = Collect.linkMapOf("server", "abc");
        position1 = Collect.linkMapOf("file", "x.log", "position", 100L, "entry", 1);
        record1 = new HistoryRecord(source1, position1, "db", "CREATE TABLE foo ( first VARCHAR(22) NOT NULL );");
        
        source2 = Collect.linkMapOf("server", "abc");
        position2 = Collect.linkMapOf("file", "x.log", "position", 300L, "entry", 2);
        record2 = new HistoryRecord(source2, position2, "db", "DROP TABLE foo;");
        
        source3 = Collect.linkMapOf("server", "xyx");
        position3 = Collect.linkMapOf("file", "y.log", "position", 10000L, "entry", 1);
        record3 = new HistoryRecord(source3, position3, "other", "DROP TABLE foo;");
    }
    
    @Test
    public void shouldConsiderOneSourceTheSame() {
        assertThat(record1.hasSameSource(record1)).isTrue();
        assertThat(record2.hasSameSource(record2)).isTrue();
        assertThat(record3.hasSameSource(record3)).isTrue();
    }

    @Test
    public void shouldConsiderTwoDifferentSourcesNotSame() {
        assertThat(record1.hasSameSource(null)).isFalse();
        assertThat(record1.hasSameSource(record3)).isFalse();
        assertThat(record2.hasSameSource(record3)).isFalse();
    }

    @Test
    public void shouldConsiderTwoDifferentSourcesTheSame() {
        assertThat(record1.hasSameSource(record2)).isTrue();
    }

    @Test
    public void shouldConsiderOneDatabaseTheSame() {
        assertThat(record1.hasSameDatabase(record1)).isTrue();
    }

    @Test
    public void shouldConsiderTwoDifferentDatabasesNotSame() {
        assertThat(record1.hasSameDatabase(record3)).isFalse();
        assertThat(record2.hasSameDatabase(record3)).isFalse();
    }

    @Test
    public void shouldCorrectlyComparePositions() {
        assertThat(record1.isAtOrBefore(record1)).isTrue();
        assertThat(record2.isAtOrBefore(record2)).isTrue();
        assertThat(record1.isAtOrBefore(record2)).isTrue();
        assertThat(record2.isAtOrBefore(record1)).isFalse();
    }
}
