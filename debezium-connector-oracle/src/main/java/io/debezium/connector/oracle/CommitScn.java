/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;

/**
 * Represents either a single or a collection of commit {@link Scn} positions that collectively
 * represents the high-watermark point for streaming changes.
 *
 * In a standalone Oracle environment, a commit {@link Scn} would normally represent a single position or
 * system change number in the logs as there is only ever a single redo thread. However, in an Oracle RAC
 * environment where each node maintains its own redo, there are multiple redo threads which maintain
 * their own "commit" point in the logs that may differ.
 *
 * This class is meant to encapsulate the Oracle RAC environment by exposing a "commit scn" as a single
 * representation that spans all nodes within the cluster as one logical unit, much like what we expect
 * when integrating with a standalone Oracle database.
 *
 * @author Chris Cranford
 */
public class CommitScn implements Comparable<Scn> {

    public static final String ROLLBACK_SEGMENT_ID_KEY = "rs_id";
    public static final String SQL_SEQUENCE_NUMBER_KEY = "ssn";
    public static final String REDO_THREAD_KEY = "redo_thread";

    // Explicitly use TreeMap to guarantee output render order
    private final Map<Integer, RedoThreadCommitScn> redoThreadCommitScns = new TreeMap<>();

    private CommitScn(Set<RedoThreadCommitScn> commitScns) {
        for (RedoThreadCommitScn commitScn : commitScns) {
            redoThreadCommitScns.put(commitScn.getThread(), commitScn);
        }
    }

    /**
     * Examines all redo threads and returns the maximum committed scn.
     *
     * @return the maximum recorded commit across all redo threads
     */
    public Scn getMaxCommittedScn() {
        return redoThreadCommitScns.values().stream().map(RedoThreadCommitScn::getCommitScn).max(Scn::compareTo).orElse(Scn.NULL);
    }

    /**
     * Get the commit scns associated with all redo threads.
     *
     * @return a map by redo thread with each commit system change number.
     */
    public Map<Integer, Scn> getCommitScnForAllRedoThreads() {
        final Map<Integer, Scn> result = new HashMap<>();
        for (Map.Entry<Integer, RedoThreadCommitScn> entry : redoThreadCommitScns.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getCommitScn());
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * Get the commit scn associated with a specific redo thread.
     *
     * @param thread the redo thread
     * @return the commit scn associated with redo thread
     */
    public Scn getCommitScnForRedoThread(int thread) {
        final RedoThreadCommitScn commitScn = redoThreadCommitScns.get(thread);
        return commitScn != null ? commitScn.getCommitScn() : Scn.NULL;
    }

    /**
     * Checks whether the transaction associated with the commit event has been handled.
     *
     * @param row the transaction commit event, should never be {@code null}
     * @return true if the commit has been handled, false if it has not
     */
    public boolean hasCommitAlreadyBeenHandled(LogMinerEventRow row) {
        final RedoThreadCommitScn commitScn = redoThreadCommitScns.get(row.getThread());
        if (commitScn != null) {
            return commitScn.getCommitScn().compareTo(row.getScn()) >= 0;
        }
        return false;
    }

    /**
     * Records the specified commit in the commit scn
     *
     * @param row the commit event, should never be {@code null}
     */
    public void recordCommit(LogMinerEventRow row) {
        final RedoThreadCommitScn redoCommitScn = redoThreadCommitScns.get(row.getThread());
        if (redoCommitScn != null) {
            redoCommitScn.setCommitScn(row.getScn());
            redoCommitScn.setRsId(row.getRsId());
            redoCommitScn.setSsn(row.getSsn());
        }
        else {
            redoThreadCommitScns.put(row.getThread(), new RedoThreadCommitScn(row));
        }
    }

    /**
     * Set the commit scn across all redo threads.
     *
     * @param commitScn the commit scn to be set, should not be {@code null}
     */
    public void setCommitScnOnAllThreads(Scn commitScn) {
        for (RedoThreadCommitScn redoCommitScn : redoThreadCommitScns.values()) {
            redoCommitScn.setCommitScn(commitScn);
        }
    }

    @Override
    public int compareTo(Scn scn) {
        if (redoThreadCommitScns.isEmpty()) {
            return Scn.NULL.compareTo(scn);
        }

        int result = 1;
        for (RedoThreadCommitScn commitScn : redoThreadCommitScns.values()) {
            int check = commitScn.getCommitScn().compareTo(scn);
            if (check < result) {
                result = check;
            }
        }
        return result;
    }

    /**
     * Store the contents of the CommitScn in the connector offsets.
     *
     * @param offset the offsets, should not be {@code null}
     * @return the adjusted offsets
     */
    public Map<String, Object> store(Map<String, Object> offset) {
        offset.put(SourceInfo.COMMIT_SCN_KEY, toCommaSeparatedValue());
        return offset;
    }

    /**
     * Store the contents of the CommitScn in the source info struct.
     *
     * @param sourceInfo the connector's source info data
     * @param sourceInfoStruct the source info struct
     * @return the adjusted source info struct
     */
    public Struct store(SourceInfo sourceInfo, Struct sourceInfoStruct) {
        if (sourceInfo.getRedoThread() != null) {
            final RedoThreadCommitScn redoThreadCommitScn = redoThreadCommitScns.get(sourceInfo.getRedoThread());
            if (redoThreadCommitScn != null) {
                if (redoThreadCommitScn.getCommitScn() != null && !redoThreadCommitScn.getCommitScn().isNull()) {
                    sourceInfoStruct.put(SourceInfo.COMMIT_SCN_KEY, redoThreadCommitScn.getCommitScn().toString());
                }

                if (redoThreadCommitScn.getRsId() != null) {
                    sourceInfoStruct.put(ROLLBACK_SEGMENT_ID_KEY, redoThreadCommitScn.getRsId());
                }

                sourceInfoStruct.put(SQL_SEQUENCE_NUMBER_KEY, redoThreadCommitScn.getSsn());
                sourceInfoStruct.put(REDO_THREAD_KEY, redoThreadCommitScn.getThread());
            }
        }
        return sourceInfoStruct;
    }

    @Override
    public String toString() {
        return "CommitScn [redoThreadCommitScns=" + redoThreadCommitScns + "]";
    }

    /**
     * Parses a string-based representation of commit scn entries as a CommitScn instance.
     *
     * @param value the commit scn entries, comma-separated
     * @return the commit scn instance, never null
     */
    public static CommitScn valueOf(String value) {
        final Set<RedoThreadCommitScn> scns = new HashSet<>();
        if (value != null) {
            final String[] parts = value.split(",");
            for (int i = 0; i < parts.length; ++i) {
                final String part = parts[i];
                scns.add(RedoThreadCommitScn.valueOf(part));
            }
        }
        return new CommitScn(scns);
    }

    /**
     * Parses a long-based representation of commit scn entries as a CommitScn instance.
     *
     * @param value the commit scn long value, should never be {@code null}
     * @return the commit scn instance, never null
     */
    public static CommitScn valueOf(Long value) {
        final Set<RedoThreadCommitScn> scns = new HashSet<>();
        if (value != null) {
            scns.add(new RedoThreadCommitScn(1, Scn.valueOf(value), null, 0));
        }
        return new CommitScn(scns);
    }

    /**
     * Load the CommitScn values from the offsets.
     *
     * @param offset the connector offsets, should not be {@code null}
     * @return the commit scn instance, never {@code null}
     */
    public static CommitScn load(Map<String, ?> offset) {
        Object value = offset.get(SourceInfo.COMMIT_SCN_KEY);
        if (value instanceof String) {
            return CommitScn.valueOf((String) value);
        }
        else if (value != null) {
            // This might be a legacy offset being read when the values were Long data types.
            // In this case, we can assume that the redo thread is 1 and explicitly create a
            // redo thread entry for it.
            return CommitScn.valueOf((Long) value);
        }
        // return a commit scn instance with no redo thread data.
        return new CommitScn(Collections.emptySet());
    }

    public static SchemaBuilder schemaBuilder(SchemaBuilder schemaBuilder) {
        return schemaBuilder.field(ROLLBACK_SEGMENT_ID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SQL_SEQUENCE_NUMBER_KEY, Schema.OPTIONAL_INT32_SCHEMA)
                .field(REDO_THREAD_KEY, Schema.OPTIONAL_INT32_SCHEMA);
    }

    /**
     * Returns the commit scn as a comma-separated list of string values.
     */
    private String toCommaSeparatedValue() {
        if (!redoThreadCommitScns.isEmpty()) {
            return redoThreadCommitScns.values().stream()
                    .map(RedoThreadCommitScn::getFormattedString)
                    .collect(Collectors.joining(","));
        }
        return null;
    }

    /**
     * Represents a commit {@link Scn} for a specific redo thread.
     */
    public static class RedoThreadCommitScn {

        private final int thread;
        private Scn commitScn;
        private String rsId;
        private long ssn;

        public RedoThreadCommitScn(int thread) {
            this(thread, Scn.NULL, null, 0);
        }

        public RedoThreadCommitScn(LogMinerEventRow row) {
            this(row.getThread(), row.getScn(), row.getRsId(), row.getSsn());
        }

        public RedoThreadCommitScn(int thread, Scn commitScn, String rsId, long ssn) {
            this.thread = thread;
            this.commitScn = commitScn;
            this.rsId = rsId;
            this.ssn = ssn;
        }

        public int getThread() {
            return thread;
        }

        public Scn getCommitScn() {
            return commitScn;
        }

        public void setCommitScn(Scn commitScn) {
            this.commitScn = commitScn;
        }

        public String getRsId() {
            return rsId;
        }

        public void setRsId(String rsId) {
            this.rsId = rsId;
        }

        public long getSsn() {
            return ssn;
        }

        public void setSsn(long ssn) {
            this.ssn = ssn;
        }

        public String getFormattedString() {
            return commitScn.toString() + ":" + (rsId != null ? rsId : "") + ":" + ssn + ":" + thread;
        }

        public static RedoThreadCommitScn valueOf(String value) {
            final String[] parts = value.split(":");
            if (parts.length == 1) {
                // Reading a legacy commit_scn entry that has only the SCN bit
                // Create the redo thread entry with thread 1.
                // There is only ever a single redo thread commit entry in this use case.
                return new RedoThreadCommitScn(1, Scn.valueOf(parts[0]), null, 0);
            }
            else if (parts.length == 4) {
                // The new redo-thread based commit scn entry
                final Scn scn = Scn.valueOf(parts[0]);
                final String rsId = parts[1];
                final long ssn = Long.parseLong(parts[2]);
                final int thread = Integer.parseInt(parts[3]);
                return new RedoThreadCommitScn(thread, scn, rsId, ssn);
            }
            throw new DebeziumException("An unexpected redo thread commit scn entry: '" + value + "'");
        }
    }
}
