/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.debezium.annotation.Immutable;

/**
 * Provides a consistent view in the Oracle database redo thread state from {@code GV$THREAD}.
 *
 * @author Chris Cranford
 */
@Immutable
public class RedoThreadState {

    private final List<RedoThread> threads;

    private RedoThreadState(List<RedoThread> threads) {
        this.threads = Collections.unmodifiableList(threads);
    }

    /**
     * Get the collection of underlying redo threads.
     * @return list of all configured redo threads
     */
    public List<RedoThread> getThreads() {
        return threads;
    }

    public RedoThread getRedoThread(Integer threadId) {
        return getThreads().stream().filter(t -> t.getThreadId().equals(threadId)).findFirst().orElse(null);
    }

    /**
     * Construct a builder for creating the redo thread state.
     * @return a new builder instance, never {@code null}
     */
    public static RedoThreadState.Builder builder() {
        return new RedoThreadState.Builder();
    }

    /**
     * Simple builder pattern for creating the immutable {@link RedoThreadState} object.
     */
    public static class Builder {
        private final List<RedoThread> threads = new ArrayList<>();

        public RedoThread.Builder thread() {
            return new RedoThread.Builder(this);
        }

        public RedoThreadState build() {
            return new RedoThreadState(threads);
        }
    }

    /**
     * Provides details about a specific redo thread in {@code GV$THREAD}.
     */
    @Immutable
    public static class RedoThread {

        private static final String OPEN = "OPEN";
        private static final String DISABLED = "DISABLED";

        private final Integer threadId;
        private final String status;
        private final String enabled;
        private final Long logGroups;
        private final String instanceName;
        private final Instant openTime;
        private final Long currentGroupNumber;
        private final Long currentSequenceNumber;
        private final Scn checkpointScn;
        private final Instant checkpointTime;
        private final Scn enabledScn;
        private final Instant enabledTime;
        private final Scn disabledScn;
        private final Instant disabledTime;
        private final Long lastRedoSequenceNumber;
        private final Long lastRedoBlock;
        private final Scn lastRedoScn;
        private final Instant lastRedoTime;
        private final Long conId;

        public RedoThread(Integer threadId, String status, String enabled, Long logGroups, String instanceName,
                          Instant openTime, Long currentGroupNumber, Long currentSequenceNumber, Scn checkpointScn,
                          Instant checkpointTime, Scn enabledScn, Instant enabledTime, Scn disabledScn, Instant disabledTime,
                          Long lastRedoSequenceNumber, Long lastRedoBlock, Scn lastRedoScn, Instant lastRedoTime, Long conId) {
            this.threadId = threadId;
            this.status = status;
            this.enabled = enabled;
            this.logGroups = logGroups;
            this.instanceName = instanceName;
            this.openTime = openTime;
            this.currentGroupNumber = currentGroupNumber;
            this.currentSequenceNumber = currentSequenceNumber;
            this.checkpointScn = checkpointScn;
            this.checkpointTime = checkpointTime;
            this.enabledScn = enabledScn;
            this.enabledTime = enabledTime;
            this.disabledScn = disabledScn;
            this.disabledTime = disabledTime;
            this.lastRedoSequenceNumber = lastRedoSequenceNumber;
            this.lastRedoBlock = lastRedoBlock;
            this.lastRedoScn = lastRedoScn;
            this.lastRedoTime = lastRedoTime;
            this.conId = conId;
        }

        public Integer getThreadId() {
            return threadId;
        }

        public String getStatus() {
            return status;
        }

        public String getEnabled() {
            return enabled;
        }

        public Long getLogGroups() {
            return logGroups;
        }

        public String getInstanceName() {
            return instanceName;
        }

        public Instant getOpenTime() {
            return openTime;
        }

        public Long getCurrentGroupNumber() {
            return currentGroupNumber;
        }

        public Long getCurrentSequenceNumber() {
            return currentSequenceNumber;
        }

        public Scn getCheckpointScn() {
            return checkpointScn;
        }

        public Instant getCheckpointTime() {
            return checkpointTime;
        }

        public Scn getEnabledScn() {
            return enabledScn;
        }

        public Instant getEnabledTime() {
            return enabledTime;
        }

        public Scn getDisabledScn() {
            return disabledScn;
        }

        public Instant getDisabledTime() {
            return disabledTime;
        }

        // Not in Oracle 10
        public Long getLastRedoSequenceNumber() {
            return lastRedoSequenceNumber;
        }

        // Not in Oracle 10
        public Long getLastRedoBlock() {
            return lastRedoBlock;
        }

        // Not in Oracle 10
        public Scn getLastRedoScn() {
            return lastRedoScn;
        }

        // Not in Oracle 10
        public Instant getLastRedoTime() {
            return lastRedoTime;
        }

        // Not in Oracle 10
        public Long getConId() {
            return conId;
        }

        public boolean isOpen() {
            return OPEN.equals(status);
        }

        public boolean isDisabled() {
            return DISABLED.equals(enabled);
        }

        @Override
        public String toString() {
            return "RedoThread{" +
                    "threadId=" + threadId +
                    ", status='" + status + '\'' +
                    ", enabled='" + enabled + '\'' +
                    ", logGroups=" + logGroups +
                    ", instanceName='" + instanceName + '\'' +
                    ", openTime=" + openTime +
                    ", currentGroupNumber=" + currentGroupNumber +
                    ", currentSequenceNumber=" + currentSequenceNumber +
                    ", checkpointScn=" + checkpointScn +
                    ", checkpointTime=" + checkpointTime +
                    ", enabledScn=" + enabledScn +
                    ", enabledTime=" + enabledTime +
                    ", disabledScn=" + disabledScn +
                    ", disabledTime=" + disabledTime +
                    ", lastRedoSequenceNumber=" + lastRedoSequenceNumber +
                    ", lastRedoBlock=" + lastRedoBlock +
                    ", lastRedoScn=" + lastRedoScn +
                    ", lastRedoTime=" + lastRedoTime +
                    ", conId=" + conId +
                    '}';
        }

        public static class Builder {
            private final RedoThreadState.Builder builder;

            private Integer threadId;
            private String status;
            private String enabled;
            private Long logGroups;
            private String instanceName;
            private Instant openTime;
            private Long currentGroupNumber;
            private Long currentSequenceNumber;
            private Scn checkpointScn;
            private Instant checkpointTime;
            private Scn enabledScn;
            private Instant enabledTime;
            private Scn disabledScn;
            private Instant disabledTime;
            private Long lastRedoSequenceNumber;
            private Long lastRedoBlock;
            private Scn lastRedoScn;
            private Instant lastRedoTime;
            private Long conId;

            public Builder(RedoThreadState.Builder builder) {
                this.builder = builder;
            }

            public Builder threadId(Integer threadId) {
                this.threadId = threadId;
                return this;
            }

            public Builder status(String status) {
                this.status = status;
                return this;
            }

            public Builder enabled(String enabled) {
                this.enabled = enabled;
                return this;
            }

            public Builder logGroups(Long logGroups) {
                this.logGroups = logGroups;
                return this;
            }

            public Builder instanceName(String instanceName) {
                this.instanceName = instanceName;
                return this;
            }

            public Builder openTime(Instant openTime) {
                this.openTime = openTime;
                return this;
            }

            public Builder currentGroupNumber(Long currentGroupNumber) {
                this.currentGroupNumber = currentGroupNumber;
                return this;
            }

            public Builder currentSequenceNumber(Long currentSequenceNumber) {
                this.currentSequenceNumber = currentSequenceNumber;
                return this;
            }

            public Builder checkpointScn(Scn checkpointScn) {
                this.checkpointScn = checkpointScn;
                return this;
            }

            public Builder checkpointTime(Instant checkpointTime) {
                this.checkpointTime = checkpointTime;
                return this;
            }

            public Builder enabledScn(Scn enabledScn) {
                this.enabledScn = enabledScn;
                return this;
            }

            public Builder enabledTime(Instant enabledTime) {
                this.enabledTime = enabledTime;
                return this;
            }

            public Builder disabledScn(Scn disabledScn) {
                this.disabledScn = disabledScn;
                return this;
            }

            public Builder disabledTime(Instant disabledTime) {
                this.disabledTime = disabledTime;
                return this;
            }

            public Builder lastRedoSequenceNumber(Long lastRedoSequenceNumber) {
                this.lastRedoSequenceNumber = lastRedoSequenceNumber;
                return this;
            }

            public Builder lastRedoBlock(Long lastRedoBlock) {
                this.lastRedoBlock = lastRedoBlock;
                return this;
            }

            public Builder lastRedoScn(Scn lastRedoScn) {
                this.lastRedoScn = lastRedoScn;
                return this;
            }

            public Builder lastRedoTime(Instant lastRedoTime) {
                this.lastRedoTime = lastRedoTime;
                return this;
            }

            public Builder conId(Long conId) {
                this.conId = conId;
                return this;
            }

            public RedoThreadState.Builder build() {
                final RedoThread thread = new RedoThread(
                        threadId, status, enabled, logGroups, instanceName, openTime, currentGroupNumber,
                        currentSequenceNumber, checkpointScn, checkpointTime, enabledScn, enabledTime, disabledScn,
                        disabledTime, lastRedoSequenceNumber, lastRedoBlock, lastRedoScn, lastRedoTime, conId);

                builder.threads.add(thread);
                return builder;
            }
        }
    }
}
