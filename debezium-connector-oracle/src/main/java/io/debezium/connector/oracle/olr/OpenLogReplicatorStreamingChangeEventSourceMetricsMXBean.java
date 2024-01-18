/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import java.math.BigInteger;

import io.debezium.connector.oracle.OracleCommonStreamingChangeEventSourceMetricsMXBean;

/**
 * Oracle Streaming Metrics for OpenLogReplicator.
 *
 * @author Chris Cranford
 */
public interface OpenLogReplicatorStreamingChangeEventSourceMetricsMXBean
        extends OracleCommonStreamingChangeEventSourceMetricsMXBean {

    /**
     * @return checkpoint scn where the connector resumes on restart
     */
    BigInteger getCheckpointScn();

    /**
     * @return checkpoint index, resume position within a checkpoint block
     */
    long getCheckpointIndex();

    /**
     * @return number of events processed from OpenLogReplicator
     */
    long getProcessedEventCount();

}
