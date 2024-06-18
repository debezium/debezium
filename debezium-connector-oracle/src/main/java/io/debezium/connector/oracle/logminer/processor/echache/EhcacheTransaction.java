/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.echache;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.processor.AbstractTransaction;
import io.debezium.connector.oracle.logminer.processor.infinispan.marshalling.VisibleForMarshalling;

public class EhcacheTransaction extends AbstractTransaction {

    private int numberOfEvents;

    public EhcacheTransaction(String transactionId, Scn startScn, Instant changeTime, String userName, Integer redoThreadId) {
        super(transactionId, startScn, changeTime, userName, redoThreadId);
        start();
    }

    @JsonCreator
    @VisibleForMarshalling
    public EhcacheTransaction(
                              @JsonProperty("transactionId") String transactionId,
                              @JsonProperty("startScn") Long startScn,
                              @JsonProperty("changeTime") Instant changeTime,
                              @JsonProperty("userName") String userName,
                              @JsonProperty("nextEventId") int numberOfEvents,
                              @JsonProperty("redoThreadId") Integer redoThreadId) {
        this(transactionId, Scn.valueOf(startScn), changeTime, userName, redoThreadId);
        this.numberOfEvents = numberOfEvents;
    }

    @Override
    public int getNumberOfEvents() {
        return numberOfEvents;
    }

    @Override
    public int getNextEventId() {
        return numberOfEvents++;
    }

    @Override
    public void start() {
        numberOfEvents = 0;
    }

    // @Override
    @JsonGetter("startScn")
    public Long getStartScnLong() {
        return super.getStartScn().isNull() ? null : super.getStartScn().longValue();
    }

    @Override
    public String toString() {
        return "EhcacheTransaction{" +
                "numberOfEvents=" + numberOfEvents +
                "} " + super.toString();
    }
}
