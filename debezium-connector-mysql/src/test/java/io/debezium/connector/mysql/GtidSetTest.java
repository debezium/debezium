/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import io.debezium.connector.mysql.GtidSet.Interval;
import io.debezium.connector.mysql.GtidSet.UUIDSet;
import io.debezium.util.Collect;

/**
 * @author Randall Hauch
 *
 */
public class GtidSetTest {

    private static final String UUID1 = "24bc7850-2c16-11e6-a073-0242ac110002";

    private GtidSet gtids;

    @Test
    public void shouldCreateSetWithSingleInterval() {
        gtids = new GtidSet(UUID1 + ":1-191");
        asertIntervalCount(UUID1, 1);
        asertIntervalExists(UUID1, 1, 191);
        asertFirstInterval(UUID1, 1, 191);
        asertLastInterval(UUID1, 1, 191);
        assertThat(gtids.toString()).isEqualTo(UUID1 + ":1-191");
    }

    @Test
    public void shouldCollapseAdjacentIntervals() {
        gtids = new GtidSet(UUID1 + ":1-191:192-199");
        asertIntervalCount(UUID1, 1);
        asertIntervalExists(UUID1, 1, 199);
        asertFirstInterval(UUID1, 1, 199);
        asertLastInterval(UUID1, 1, 199);
        assertThat(gtids.toString()).isEqualTo(UUID1 + ":1-199");
    }

    @Test
    public void shouldNotCollapseNonAdjacentIntervals() {
        gtids = new GtidSet(UUID1 + ":1-191:193-199");
        asertIntervalCount(UUID1, 2);
        asertFirstInterval(UUID1, 1, 191);
        asertLastInterval(UUID1, 193, 199);
        assertThat(gtids.toString()).isEqualTo(UUID1 + ":1-191:193-199");
    }

    @Test
    public void shouldCreateWithMultipleIntervals() {
        gtids = new GtidSet(UUID1 + ":1-191:193-199:1000-1033");
        asertIntervalCount(UUID1, 3);
        asertFirstInterval(UUID1, 1, 191);
        asertIntervalExists(UUID1, 193, 199);
        asertLastInterval(UUID1, 1000, 1033);
        assertThat(gtids.toString()).isEqualTo(UUID1 + ":1-191:193-199:1000-1033");
    }

    @Test
    public void shouldCreateWithMultipleIntervalsThatMayBeAdjacent() {
        gtids = new GtidSet(UUID1 + ":1-191:192-199:1000-1033:1035-1036:1038-1039");
        asertIntervalCount(UUID1, 4);
        asertFirstInterval(UUID1, 1, 199);
        asertIntervalExists(UUID1, 1000, 1033);
        asertIntervalExists(UUID1, 1035, 1036);
        asertLastInterval(UUID1, 1038, 1039);
        assertThat(gtids.toString()).isEqualTo(UUID1 + ":1-199:1000-1033:1035-1036:1038-1039"); // ??
    }

    @Test
    public void shouldCorrectlyDetermineIfSimpleGtidSetIsContainedWithinAnother() {
        gtids = new GtidSet("7c1de3f2-3fd2-11e6-9cdc-42010af000bc:1-41");
        assertThat(gtids.isContainedWithin(new GtidSet("7c1de3f2-3fd2-11e6-9cdc-42010af000bc:1-41"))).isTrue();
        assertThat(gtids.isContainedWithin(new GtidSet("7c1de3f2-3fd2-11e6-9cdc-42010af000bc:1-42"))).isTrue();
        assertThat(gtids.isContainedWithin(new GtidSet("7c1de3f2-3fd2-11e6-9cdc-42010af000bc:2-41"))).isFalse();
        assertThat(gtids.isContainedWithin(new GtidSet("7145bf69-d1ca-11e5-a588-0242ac110004:1"))).isFalse();
    }

    @Test
    public void shouldCorrectlyDetermineIfComplexGtidSetIsContainedWithinAnother() {
        GtidSet connector = new GtidSet("036d85a9-64e5-11e6-9b48-42010af0000c:1-2,"
                + "7145bf69-d1ca-11e5-a588-0242ac110004:1-3200,"
                + "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:1-41");
        GtidSet server = new GtidSet("036d85a9-64e5-11e6-9b48-42010af0000c:1-2,"
                + "7145bf69-d1ca-11e5-a588-0242ac110004:1-3202,"
                + "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:1-41");
        assertThat(connector.isContainedWithin(server)).isTrue();
    }

    @Test
    public void shouldCorrectlyDetermineIfComplexGtidSetIsContainedWithinBrokenRanges() {
        GtidSet server = new GtidSet("7388bf7d-2aac-11ed-9237-021082410453:1-351497416,"
                + "7d187d03-4f6c-11ed-a5ac-0e393478d1e3:1-1291577186,"
                + "820d71d6-eeea-11ec-95a8-0e2ad29b0edb:1-487962549,"
                + "9475ad1f-a911-11ed-ae57-0e3cf2efaa99:1-1128180377:1325715925-1325725070,"
                + "d6853f74-3bdf-11e9-9867-0a635193bf30:1-1074470477,"
                + "e6330d3c-cd8d-11ec-bf02-02d395688cf9:1-568778112,");
        GtidSet connector = new GtidSet("9475ad1f-a911-11ed-ae57-0e3cf2efaa99:1325715925-1325725070");

        assertThat(connector.isContainedWithin(server)).isTrue();
    }

    @Test
    public void shouldCorrectlyDetermineIfComplexGtidSetWithVariousLineSeparatorsIsContainedWithinAnother() {
        GtidSet connector = new GtidSet("036d85a9-64e5-11e6-9b48-42010af0000c:1-2,"
                + "7145bf69-d1ca-11e5-a588-0242ac110004:1-3200,"
                + "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:1-41");
        Arrays.stream(new String[]{ "\r\n", "\n", "\r" })
                .forEach(separator -> {
                    GtidSet server = new GtidSet("036d85a9-64e5-11e6-9b48-42010af0000c:1-2," + separator +
                            "7145bf69-d1ca-11e5-a588-0242ac110004:1-3202," + separator +
                            "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:1-41");
                    assertThat(connector.isContainedWithin(server)).isTrue();
                });
    }

    @Test
    public void shouldFilterServerUuids() {
        String gtidStr = "036d85a9-64e5-11e6-9b48-42010af0000c:1-2,"
                + "7145bf69-d1ca-11e5-a588-0242ac110004:1-3200,"
                + "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:1-41";
        Collection<String> keepers = Collect.arrayListOf("036d85a9-64e5-11e6-9b48-42010af0000c",
                "7c1de3f2-3fd2-11e6-9cdc-42010af000bc",
                "wont-be-found");
        GtidSet original = new GtidSet(gtidStr);
        assertThat(original.forServerWithId("036d85a9-64e5-11e6-9b48-42010af0000c")).isNotNull();
        assertThat(original.forServerWithId("7c1de3f2-3fd2-11e6-9cdc-42010af000bc")).isNotNull();
        assertThat(original.forServerWithId("7145bf69-d1ca-11e5-a588-0242ac110004")).isNotNull();

        GtidSet filtered = original.retainAll(keepers::contains);
        List<String> actualUuids = filtered.getUUIDSets().stream().map(UUIDSet::getUUID).collect(Collectors.toList());
        assertThat(keepers.containsAll(actualUuids)).isTrue();
        assertThat(filtered.forServerWithId("7145bf69-d1ca-11e5-a588-0242ac110004")).isNull();
    }

    @Test
    public void subtract() {
        String gtidStr1 = "036d85a9-64e5-11e6-9b48-42010af0000c:1-20,"
                + "7145bf69-d1ca-11e5-a588-0242ac110004:1-3200:3400-3800:3900-3990,"
                + "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:5-8:12-18:25-55:60-65";
        String gtidStr2 = "036d85a9-64e5-11e6-9b48-42010af0000c:1-21,"
                + "7145bf69-d1ca-11e5-a588-0242ac110004:1-3200:3400-3800:4500,"
                + "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:1-49:62-70:80-100";
        String diff = "036d85a9-64e5-11e6-9b48-42010af0000c:21,"
                + "7145bf69-d1ca-11e5-a588-0242ac110004:4500,"
                + "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:1-4:9-11:19-24:66-70:80-100";
        GtidSet gtidSet1 = new GtidSet(gtidStr1);
        GtidSet gtidSet2 = new GtidSet(gtidStr2);

        GtidSet gtidSetDiff = gtidSet2.subtract(gtidSet1);
        GtidSet expectedDiff = new GtidSet(diff);
        assertThat(gtidSetDiff).isEqualTo(expectedDiff);
    }

    @Test
    public void removeInterval() {
        Interval interval1 = new Interval(3, 7);
        Interval interval2 = new Interval(2, 5);
        Interval interval3 = new Interval(4, 5);
        Interval interval4 = new Interval(9, 12);
        Interval interval5 = new Interval(0, 2);
        assertThat(interval1.remove(interval2)).isEqualTo(Collections.singletonList(new Interval(6, 7)));
        assertThat(interval2.remove(interval1)).isEqualTo(Collections.singletonList(new Interval(2, 2)));
        assertThat(interval1.remove(interval3)).isEqualTo(Arrays.asList(new Interval(3, 3), new Interval(6, 7)));
        assertThat(interval1.remove(interval4)).isEqualTo(Collections.singletonList(interval1));
        assertThat(interval1.remove(interval5)).isEqualTo(Collections.singletonList(interval1));
        assertThat(interval3.remove(interval1)).isEqualTo(Collections.emptyList());
        assertThat(interval3.remove(interval3)).isEqualTo(Collections.emptyList());
    }

    @Test
    public void removeAllIntervals() {
        Interval interval = new Interval(1, 49);
        List<Interval> intervalsToRemove = Arrays.asList(new Interval(5, 8), new Interval(12, 18), new Interval(25, 55), new Interval(60, 65));
        List<Interval> diff = Arrays.asList(new Interval(1, 4), new Interval(9, 11), new Interval(19, 24));
        assertThat(interval.removeAll(intervalsToRemove)).isEqualTo(diff);
    }

    protected void asertIntervalCount(String uuid, int count) {
        UUIDSet set = gtids.forServerWithId(uuid);
        assertThat(set.getIntervals().size()).isEqualTo(count);
    }

    protected void asertIntervalExists(String uuid, int start, int end) {
        assertThat(hasInterval(uuid, start, end)).isTrue();
    }

    protected void asertFirstInterval(String uuid, int start, int end) {
        UUIDSet set = gtids.forServerWithId(uuid);
        Interval interval = set.getIntervals().iterator().next();
        assertThat(interval.getStart()).isEqualTo(start);
        assertThat(interval.getEnd()).isEqualTo(end);
    }

    protected void asertLastInterval(String uuid, int start, int end) {
        UUIDSet set = gtids.forServerWithId(uuid);
        Interval interval = new LinkedList<>(set.getIntervals()).getLast();
        assertThat(interval.getStart()).isEqualTo(start);
        assertThat(interval.getEnd()).isEqualTo(end);
    }

    protected boolean hasInterval(String uuid, int start, int end) {
        UUIDSet set = gtids.forServerWithId(uuid);
        for (Interval interval : set.getIntervals()) {
            if (interval.getStart() == start && interval.getEnd() == end) {
                return true;
            }
        }
        return false;
    }

}
