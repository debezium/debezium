/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Objects;

import org.bson.BsonTimestamp;

/**
 * Handler for working with MongoDB Multi-Task offset stepwise mechanism.
 * With stepwise mechanism we track the start / stop time to manage the cursor in change stream across multiple tasks.
 * <p>
 *
 * Each task is managed deterministically based on taskId (zero indexed), taskCount, and hop size.
 * This allows DBZ to managed tasks separately and ensure that each event is only processed by one task.
 *
 * EX: 3 tasks, hop size of 5, and lastOffset of 0, will distribute the tasks based on offsetTime showing start and stop times.
 * Task 0: [0->4], [15->19], [30->34], ...
 * Task 1: [5->9], [20->24], ...
 * Task 2: [10->14], [25->29], ...
 *
 * Beyond this we have the ability to optimize the start time.
 * If the offset is in the middle of a step it will process from that point, reducing the hop size.
 * If the offset is past the stop time, it will move the next hopt
 *
 * EX: 3 tasks, hop size of 5, and lastOffset of 7, will distribute the tasks based on offsetTime showing start and stop times.
 *  * Task 0: [15->19], [30->34], ... (Skipping [0-4])
 *  * Task 1: [7->9], [20->24], ...   (Reducing the first hop from [5->9] to [7->9]
 *  * Task 2: [10->14], [25->29], ... (No changes)
 *
 * @author Anthony Noakes
 */
public class MultiTaskOffsetHandler {

    public int hopSizeSeconds;
    public int taskCount;
    public int taskId;
    public BsonTimestamp oplogStart;
    public BsonTimestamp oplogStop;
    public BsonTimestamp optimizedOplogStart;

    public boolean started;

    public MultiTaskOffsetHandler(int hopSizeSeconds, int taskCount, int taskId) {
        this.hopSizeSeconds = hopSizeSeconds;
        this.taskCount = taskCount;
        this.taskId = taskId;
        this.started = false;
    }

    public MultiTaskOffsetHandler(BsonTimestamp lastOffset, int hopSizeSeconds, int taskCount, int taskId) {
        this.hopSizeSeconds = hopSizeSeconds;
        this.taskCount = taskCount;
        this.taskId = taskId;

        startAtTimestamp(lastOffset);
    }

    public MultiTaskOffsetHandler startAtTimestamp(BsonTimestamp start) {
        oplogStart = getStepwiseOffsetStart(start, hopSizeSeconds, taskCount, taskId);
        oplogStop = getStepwiseOffsetStop(oplogStart, hopSizeSeconds);
        optimizedOplogStart = optimizeStepwiseOffsetStart(start, oplogStart);
        started = true;
        return this;
    }

    public MultiTaskOffsetHandler nextHop() {
        if (started) {
            oplogStart = getStepwiseOffsetNextStart(oplogStop, hopSizeSeconds, taskCount);
            oplogStop = getStepwiseOffsetStop(oplogStart, hopSizeSeconds);
            optimizedOplogStart = oplogStart;
        }
        return this;
    }

    public MultiTaskOffsetHandler nextHop(BsonTimestamp minTime) {
        if (!started) {
            return this;
        }
        if (minTime == null) {
            return nextHop();
        }
        oplogStart = getStepwiseOffsetNextStart(oplogStop, hopSizeSeconds, taskCount);
        if (Objects.compare(oplogStart, minTime, BsonTimestamp::compareTo) < 0) {
            oplogStart = getStepwiseOffsetStart(minTime, hopSizeSeconds, taskCount, taskId);
        }
        oplogStop = getStepwiseOffsetStop(oplogStart, hopSizeSeconds);
        optimizedOplogStart = optimizeStepwiseOffsetStart(minTime, oplogStart);
        return this;
    }

    public static BsonTimestamp getStepwiseOffsetStart(BsonTimestamp lastOffset, int hopSizeSeconds, int taskCount, int taskId) {
        long startTimestampSeconds = lastOffset.getTime();
        long stepwiseStartOffset = startTimestampSeconds - (startTimestampSeconds % (hopSizeSeconds * taskCount)) + (hopSizeSeconds * taskId);

        BsonTimestamp bsonStartTimestamp = new BsonTimestamp((int) (stepwiseStartOffset), 0);
        BsonTimestamp stepwiseStopOffset = getStepwiseOffsetStop(bsonStartTimestamp, hopSizeSeconds);

        if (lastOffset.getTime() >= stepwiseStopOffset.getTime()) {
            bsonStartTimestamp = getStepwiseOffsetNextStart(stepwiseStopOffset, hopSizeSeconds, taskCount);
        }

        return bsonStartTimestamp;
    }

    public static BsonTimestamp getStepwiseOffsetStop(BsonTimestamp stepwiseOffsetStart, int hopSizeSeconds) {
        long updatedTimestampSeconds = stepwiseOffsetStart.getTime() + hopSizeSeconds;
        BsonTimestamp updatedTimestamp = new BsonTimestamp((int) updatedTimestampSeconds, 0);
        return updatedTimestamp;
    }

    public static BsonTimestamp getStepwiseOffsetNextStart(BsonTimestamp offsetStop, int hopSizeSeconds, int taskCount) {
        long stopTimestampSeconds = offsetStop.getTime();
        long nextOffsetStart = stopTimestampSeconds + (hopSizeSeconds * (taskCount - 1));

        BsonTimestamp bsonTimestamp = new BsonTimestamp((int) nextOffsetStart, 0);
        return bsonTimestamp;
    }

    public static BsonTimestamp optimizeStepwiseOffsetStart(BsonTimestamp lastOffset, BsonTimestamp startOffset) {
        BsonTimestamp optimized = startOffset;

        if (Objects.compare(lastOffset, startOffset, BsonTimestamp::compareTo) > 0) {
            optimized = lastOffset;
        }

        return optimized;
    }
}
