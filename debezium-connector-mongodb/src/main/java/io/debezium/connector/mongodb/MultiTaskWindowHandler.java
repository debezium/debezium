/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Objects;

import org.bson.BsonTimestamp;

/**
 * Handler for working with MongoDB Multi-Task stepwise mechanism.
 * With stepwise mechanism we track the start / stop time to manage the cursor in change stream across multiple tasks.
 * <p>
 *
 * Each task is managed deterministically based on taskId (zero indexed), taskCount, and hop size.
 * This allows DBZ to managed tasks separately and ensure that each event is only processed by one task.
 *
 * EX: 3 tasks, hop size of 5, and startTime of 0, will distribute the tasks based on event timestamp in relation to start
 * and stop times.
 * Task 0: [0->4], [15->19], [30->34], ...
 * Task 1: [5->9], [20->24], ...
 * Task 2: [10->14], [25->29], ...
 *
 * Beyond this we have the ability to optimize the start time.
 * If the starting timestamp is in the middle of a step it will process from that point, reducing the hop size.
 * If the starting timestamp is past the stop time, it will skip to the next hop.
 *
 * EX: 3 tasks, hop size of 5, and startTime of 7, will distribute the tasks based on event timestamp in relation to start
 * and stop times.
 *  * Task 0: [15->19], [30->34], ... (Skipping [0-4])
 *  * Task 1: [7->9], [20->24], ...   (Reducing the first hop from [5->9] to [7->9]
 *  * Task 2: [10->14], [25->29], ... (No changes)
 *
 * @author Anthony Noakes
 */
public class MultiTaskWindowHandler {

    public int hopSizeSeconds;
    public int taskCount;
    public int taskId;
    public BsonTimestamp windowStart;
    public BsonTimestamp windowStop;
    public BsonTimestamp optimizedWindowStart;

    public boolean started;

    /**
     * Create a new {@link MultiTaskWindowHandler} starting with no starting timestamp.
     * {@link #startAtTimestamp(BsonTimestamp)} must be called before using the handler.
     * @param hopSizeSeconds
     * @param taskCount
     * @param taskId
     */
    public MultiTaskWindowHandler(int hopSizeSeconds, int taskCount, int taskId) {
        this.hopSizeSeconds = hopSizeSeconds;
        this.taskCount = taskCount;
        this.taskId = taskId;
        this.started = false;
    }

    /**
     * Create a new {@link MultiTaskWindowHandler} starting at lastOffset.
     * @param lastOffset the timestamp to start at
     * @param hopSizeSeconds window size in seconds
     * @param taskCount number of tasks in associated multi-task configuration
     * @param taskId the task id of the task using this handler
     */
    public MultiTaskWindowHandler(BsonTimestamp lastOffset, int hopSizeSeconds, int taskCount, int taskId) {
        this.hopSizeSeconds = hopSizeSeconds;
        this.taskCount = taskCount;
        this.taskId = taskId;

        startAtTimestamp(lastOffset);
    }

    /**
     * Start the window handler at the given timestamp.
     * @param start the timestamp to start at
     * @return this
     */
    public MultiTaskWindowHandler startAtTimestamp(BsonTimestamp start) {
        windowStart = getStepwiseWindowStart(start, hopSizeSeconds, taskCount, taskId);
        windowStop = getStepwiseWindowStop(windowStart, hopSizeSeconds);
        optimizedWindowStart = optimizeStepwiseWindowStart(start, windowStart);
        started = true;
        return this;
    }

    /**
     * Update {@link #windowStart} and {@link #windowStop} to the next window.
     * @return this
     */
    public MultiTaskWindowHandler nextHop() {
        if (started) {
            windowStart = getStepwiseWindowNextStart(windowStop, hopSizeSeconds, taskCount);
            windowStop = getStepwiseWindowStop(windowStart, hopSizeSeconds);
            optimizedWindowStart = windowStart;
        }
        return this;
    }

    /**
     * Update {@link #windowStart} and {@link #windowStop} to the next window. If the next window does not contain any
     * timestamp greater than or equal to minTime, update to the next window that does.
     * @param minTime the minimum timestamp that the next window must contain
     * @return this
     */
    public MultiTaskWindowHandler nextHop(BsonTimestamp minTime) {
        if (!started) {
            return this;
        }
        if (minTime == null) {
            return nextHop();
        }
        windowStart = getStepwiseWindowNextStart(windowStop, hopSizeSeconds, taskCount);
        if (Objects.compare(windowStart, minTime, BsonTimestamp::compareTo) < 0) {
            windowStart = getStepwiseWindowStart(minTime, hopSizeSeconds, taskCount, taskId);
        }
        windowStop = getStepwiseWindowStop(windowStart, hopSizeSeconds);
        optimizedWindowStart = optimizeStepwiseWindowStart(minTime, windowStart);
        return this;
    }

    /**
     * Get the starting timestamp of the window containing minStartTime. If this task is not responsible for the given timestamp
     * this function instead returns the starting timestamp of the next window after minStartTime that this task is responsible for.
     * @param minStartTime the timestamp to start at
     * @param hopSizeSeconds window size in seconds
     * @param taskCount number of tasks in associated multi-task configuration
     * @param taskId the task id of the task using this handler
     * @return the starting timestamp of either the window containing minStartTime or the first window after minStartTime that this task is responsible for
     */
    public static BsonTimestamp getStepwiseWindowStart(BsonTimestamp minStartTime, int hopSizeSeconds, int taskCount, int taskId) {
        long startTimestampSeconds = minStartTime.getTime();
        long stepwiseStartTime = startTimestampSeconds - (startTimestampSeconds % (hopSizeSeconds * taskCount)) + (hopSizeSeconds * taskId);

        BsonTimestamp bsonStartTimestamp = new BsonTimestamp((int) (stepwiseStartTime), 0);
        BsonTimestamp stepwiseStopTime = getStepwiseWindowStop(bsonStartTimestamp, hopSizeSeconds);

        if (minStartTime.getTime() >= stepwiseStopTime.getTime()) {
            bsonStartTimestamp = getStepwiseWindowNextStart(stepwiseStopTime, hopSizeSeconds, taskCount);
        }

        return bsonStartTimestamp;
    }

    /**
     * Get the stop timestamp of the window starting at stepwiseWindowStart.
     * @param stepwiseWindowStart the starting timestamp of the window
     * @param hopSizeSeconds window size in seconds
     * @return the stop timestamp of the window
     */
    public static BsonTimestamp getStepwiseWindowStop(BsonTimestamp stepwiseWindowStart, int hopSizeSeconds) {
        long updatedTimestampSeconds = stepwiseWindowStart.getTime() + hopSizeSeconds;
        BsonTimestamp updatedTimestamp = new BsonTimestamp((int) updatedTimestampSeconds, 0);
        return updatedTimestamp;
    }

    /**
     * Get the starting timestamp of the next window after windowStop that this task is responsible for.
     * @param windowStop the stop timestamp of the current window
     * @param hopSizeSeconds window size in seconds
     * @param taskCount number of tasks in associated multi-task configuration
     * @return the starting timestamp of the next window after windowStop that this task is responsible for
     */
    public static BsonTimestamp getStepwiseWindowNextStart(BsonTimestamp windowStop, int hopSizeSeconds, int taskCount) {
        long stopTimestampSeconds = windowStop.getTime();
        long nextWindowStart = stopTimestampSeconds + (hopSizeSeconds * (taskCount - 1));

        BsonTimestamp bsonTimestamp = new BsonTimestamp((int) nextWindowStart, 0);
        return bsonTimestamp;
    }

    /**
     * Compare the start of the current window against the given timestamp and return a new starting time for the current window.
     * The new starting time is not guaranteed to be less than the current stop time.
     * @param minstartTime the timestamp of the last event processed
     * @param startTime the start of the current window
     * @return the optimized start time
     */
    public static BsonTimestamp optimizeStepwiseWindowStart(BsonTimestamp minstartTime, BsonTimestamp startTime) {
        BsonTimestamp optimized = startTime;

        if (Objects.compare(minstartTime, startTime, BsonTimestamp::compareTo) > 0) {
            optimized = minstartTime;
        }

        return optimized;
    }
}
