/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static java.lang.Math.ceil;
import static org.fest.assertions.Assertions.assertThat;

import org.bson.BsonTimestamp;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Anthony Noakes
 */

public class MultiTaskOffsetHandlerTest {

    @Test
    public void initTestState() {
        Assert.assertEquals(false, new MultiTaskOffsetHandler().enabled);
        Assert.assertEquals(true, new MultiTaskOffsetHandler(new BsonTimestamp(100, 0), 10, 3, 0).enabled);
    }

    @Test
    public void shouldProcessAllRecordsSingleTask() {
        int start = 0;
        int hop = 2;
        int tasks = 1;
        int taskId = 0;
        int finalStop = 100;

        testScenario(start, finalStop, hop, tasks, taskId);
    }

    @Test
    public void shouldProcessHalfRecordsTwoTask() {
        int start = 0;
        int hop = 2;
        int tasks = 2;
        int taskId = 0;
        int finalStop = 100;

        testScenario(start, finalStop, hop, tasks, taskId);
    }

    @Test
    public void shouldProcessQuarterRecordsFourTask() {
        int start = 0;
        int hop = 10;
        int tasks = 4;
        int taskId = 0;
        int finalStop = 100;

        testScenario(start, finalStop, hop, tasks, taskId);
    }

    @Test
    public void shouldGenerateStepwiseOffsetStart() {

        // Offset it set to beginning of step
        testGetStepwiseOffsetStart(0, 0, 10, 3, 0);
        testGetStepwiseOffsetStart(10, 0, 10, 3, 1);
        testGetStepwiseOffsetStart(20, 0, 10, 3, 2);

        // Offset is set to middle of step
        MultiTaskOffsetHandler m = testGetStepwiseOffsetStart(0, 5, 10, 3, 0);
        testOptimizeStepwiseOffsetStart(5, 5, 0, m);
        testGetStepwiseOffsetStart(10, 5, 10, 3, 1);
        testGetStepwiseOffsetStart(20, 5, 10, 3, 2);

        // Offset is set to beginning of next step
        testGetStepwiseOffsetStart(30, 15, 10, 3, 0);
        testGetStepwiseOffsetStart(10, 15, 10, 3, 1);
        testGetStepwiseOffsetStart(20, 15, 10, 3, 2);

        testGetStepwiseOffsetStart(30015, 30000, 15, 3, 0);
        testGetStepwiseOffsetStart(30030, 30000, 15, 3, 1);
        testGetStepwiseOffsetStart(30000, 30000, 15, 3, 2);
    }

    private void testScenario(int start, int finalStop, int hop, int tasks, int taskId) {
        int hops = 0;
        int tempStart = start;
        while (tempStart < finalStop) {
            int stop = tempStart + hop;
            int nextStart = stop + ((tasks - 1) * hop);

            MultiTaskOffsetHandler m = testGetStepwiseOffsetStart(tempStart, tempStart, hop, tasks, taskId);
            testGetStepwiseOffsetStop(stop, tempStart, hop, m);
            testGetStepwiseOffsetNextStart(nextStart, stop, hop, tasks, m);

            tempStart = nextStart;

            hops++;
        }

        Assert.assertEquals((int) ceil((double) (finalStop - start) / (double) (hop * tasks)), hops);
    }

    private MultiTaskOffsetHandler testGetStepwiseOffsetStart(int expectedSeconds, int timeInSeconds, int hopSize, int taskCount, int taskId) {
        BsonTimestamp ts = new BsonTimestamp(timeInSeconds, 0);
        BsonTimestamp bt = MultiTaskOffsetHandler.getStepwiseOffsetStart(ts, hopSize, taskCount, taskId);

        MultiTaskOffsetHandler m = new MultiTaskOffsetHandler(ts, hopSize, taskCount, taskId);

        long t = bt.getTime();
        assertThat(t).isEqualTo(expectedSeconds);
        assertThat(m.oplogStart.getTime()).isEqualTo(expectedSeconds);

        return m;
    }

    private void testOptimizeStepwiseOffsetStart(int expectedSeconds, int timeInSeconds, int startInSeconds, MultiTaskOffsetHandler m) {
        BsonTimestamp opBt = new BsonTimestamp(timeInSeconds, 0);
        BsonTimestamp startBt = new BsonTimestamp(startInSeconds, 0);
        BsonTimestamp bt = MultiTaskOffsetHandler.optimizeStepwiseOffsetStart(opBt, startBt);

        long t = bt.getTime();
        assertThat(t).isEqualTo(expectedSeconds);
        assertThat(m.optimizedOplogStart.getTime()).isEqualTo(expectedSeconds);

    }

    private void testGetStepwiseOffsetStop(int expectedSeconds, int offsetStartInSeconds, int hopSize, MultiTaskOffsetHandler m) {
        BsonTimestamp ts = new BsonTimestamp(offsetStartInSeconds, 0);
        BsonTimestamp bt = MultiTaskOffsetHandler.getStepwiseOffsetStop(ts, hopSize);

        long t = bt.getTime();
        assertThat(t).isEqualTo(expectedSeconds);
        assertThat(m.oplogStop.getTime()).isEqualTo(expectedSeconds);
    }

    private void testGetStepwiseOffsetNextStart(int expectedSeconds, int offsetStopInSeconds, int hopSize, int taskCount, MultiTaskOffsetHandler m) {
        BsonTimestamp ts = new BsonTimestamp(offsetStopInSeconds, 0);
        BsonTimestamp bt = MultiTaskOffsetHandler.getStepwiseOffsetNextStart(ts, hopSize, taskCount);

        m = m.nextHop();

        long t = bt.getTime();
        assertThat(t).isEqualTo(expectedSeconds);
        assertThat(m.oplogStart.getTime()).isEqualTo(expectedSeconds);
        assertThat(m.optimizedOplogStart.getTime()).isEqualTo(expectedSeconds);
    }
}
