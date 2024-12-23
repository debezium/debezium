/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static java.lang.Math.ceil;
import static org.assertj.core.api.Assertions.assertThat;

import org.bson.BsonTimestamp;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Anthony Noakes
 */

public class MultiTaskWindowHandlerTest {

    @Test
    public void initTestState() {
        Assert.assertFalse(new MultiTaskWindowHandler(10, 3, 0).started);
        Assert.assertTrue(new MultiTaskWindowHandler(new BsonTimestamp(100, 0), 10, 3, 0).started);
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

        // Start from beginning of step
        testGetStepwiseWindowStart(0, 0, 10, 3, 0);
        testGetStepwiseWindowStart(10, 0, 10, 3, 1);
        testGetStepwiseWindowStart(20, 0, 10, 3, 2);

        // Start from middle of step
        MultiTaskWindowHandler m = testGetStepwiseWindowStart(0, 5, 10, 3, 0);
        testOptimizeStepwiseWindowStart(5, 5, 0, m);
        testGetStepwiseWindowStart(10, 5, 10, 3, 1);
        testGetStepwiseWindowStart(20, 5, 10, 3, 2);

        // Start from beginning of next step
        testGetStepwiseWindowStart(30, 15, 10, 3, 0);
        testGetStepwiseWindowStart(10, 15, 10, 3, 1);
        testGetStepwiseWindowStart(20, 15, 10, 3, 2);

        testGetStepwiseWindowStart(30015, 30000, 15, 3, 0);
        testGetStepwiseWindowStart(30030, 30000, 15, 3, 1);
        testGetStepwiseWindowStart(30000, 30000, 15, 3, 2);
    }

    private void testScenario(int start, int finalStop, int hop, int tasks, int taskId) {
        int hops = 0;
        int tempStart = start;
        while (tempStart < finalStop) {
            int stop = tempStart + hop;
            int nextStart = stop + ((tasks - 1) * hop);

            MultiTaskWindowHandler m = testGetStepwiseWindowStart(tempStart, tempStart, hop, tasks, taskId);
            testGetStepwiseWindowStop(stop, tempStart, hop, m);
            testGetStepwiseWindowNextStart(nextStart, stop, hop, tasks, m);

            tempStart = nextStart;

            hops++;
        }

        Assert.assertEquals((int) ceil((double) (finalStop - start) / (double) (hop * tasks)), hops);
    }

    private MultiTaskWindowHandler testGetStepwiseWindowStart(int expectedSeconds, int timeInSeconds, int hopSize, int taskCount, int taskId) {
        BsonTimestamp ts = new BsonTimestamp(timeInSeconds, 0);
        BsonTimestamp bt = MultiTaskWindowHandler.getStepwiseWindowStart(ts, hopSize, taskCount, taskId);

        MultiTaskWindowHandler m = new MultiTaskWindowHandler(ts, hopSize, taskCount, taskId);

        long t = bt.getTime();
        assertThat(t).isEqualTo(expectedSeconds);
        assertThat(m.windowStart.getTime()).isEqualTo(expectedSeconds);

        return m;
    }

    private void testOptimizeStepwiseWindowStart(int expectedSeconds, int timeInSeconds, int startInSeconds, MultiTaskWindowHandler m) {
        BsonTimestamp opBt = new BsonTimestamp(timeInSeconds, 0);
        BsonTimestamp startBt = new BsonTimestamp(startInSeconds, 0);
        BsonTimestamp bt = MultiTaskWindowHandler.optimizeStepwiseWindowStart(opBt, startBt);

        long t = bt.getTime();
        assertThat(t).isEqualTo(expectedSeconds);
        assertThat(m.optimizedWindowStart.getTime()).isEqualTo(expectedSeconds);

    }

    private void testGetStepwiseWindowStop(int expectedSeconds, int windowStartInSeconds, int hopSize, MultiTaskWindowHandler m) {
        BsonTimestamp ts = new BsonTimestamp(windowStartInSeconds, 0);
        BsonTimestamp bt = MultiTaskWindowHandler.getStepwiseWindowStop(ts, hopSize);

        long t = bt.getTime();
        assertThat(t).isEqualTo(expectedSeconds);
        assertThat(m.windowStop.getTime()).isEqualTo(expectedSeconds);
    }

    private void testGetStepwiseWindowNextStart(int expectedSeconds, int windowStopInSeconds, int hopSize, int taskCount, MultiTaskWindowHandler m) {
        BsonTimestamp ts = new BsonTimestamp(windowStopInSeconds, 0);
        BsonTimestamp bt = MultiTaskWindowHandler.getStepwiseWindowNextStart(ts, hopSize, taskCount);

        m = m.nextHop();

        long t = bt.getTime();
        assertThat(t).isEqualTo(expectedSeconds);
        assertThat(m.windowStart.getTime()).isEqualTo(expectedSeconds);
        assertThat(m.optimizedWindowStart.getTime()).isEqualTo(expectedSeconds);
    }
}
