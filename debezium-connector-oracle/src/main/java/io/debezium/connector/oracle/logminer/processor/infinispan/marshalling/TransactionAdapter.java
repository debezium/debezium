/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.infinispan.marshalling;

import java.time.Instant;
import java.util.List;

import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.Transaction;

/**
 * An Infinispan ProtoStream adapter for marshalling a {@link Transaction} instance.
 *
 * This class defines a factory for creating {@link Transaction} instances when hydrating a transaction
 * record from the protocol buffer datastore as well as field handlers to extract values from a given
 * transaction instance for serializing the instance to a protocol buffer stream.
 *
 * @author Chris Cranford
 */
@ProtoAdapter(Transaction.class)
public class TransactionAdapter {

    /**
     * A ProtoStream factory that creates a {@link Transaction} instance from field values.
     *
     * @param transactionId the transaction identifier
     * @param scn the starting system change number of the transaction
     * @param changeTime the starting time of the transaction
     * @param events list of events that are part of the transaction
     * @param userName the user name
     * @param numberOfEvents the number of events in the transaction
     * @return the constructed Transaction instance
     */
    @ProtoFactory
    public Transaction factory(String transactionId, String scn, String changeTime, List<LogMinerEvent> events, String userName, int numberOfEvents) {
        return new Transaction(transactionId, Scn.valueOf(scn), Instant.parse(changeTime), events, userName, numberOfEvents);
    }

    /**
     * A ProtoStream handler to extract the {@code transactionId} field from the {@link Transaction}.
     *
     * @param transaction the transaction instance, must not be {@code null}
     * @return the transaction identifier, never {@code null}
     */
    @ProtoField(number = 1, required = true)
    public String getTransactionId(Transaction transaction) {
        return transaction.getTransactionId();
    }

    /**
     * A ProtoStream handler to extract the {@code startScn} field from the {@link Transaction}.
     *
     * @param transaction the transaction instance, must not be {@code null}
     * @return the starting system change number, never {@code null}
     */
    @ProtoField(number = 2, required = true)
    public String getScn(Transaction transaction) {
        // We intentionally serialize the Scn class as a string to the protocol buffer datastore
        // and so the factory method also accepts a string parameter and converts the value to a
        // Scn instance during instantiation. This avoids the need for an additional adapter.
        return transaction.getStartScn().toString();
    }

    /**
     * A ProtoStream handler to extract the {@code changeTime} field from the {@link Transaction}.
     *
     * @param transaction the transaction instance, must not be {@code null}
     * @return the starting time of the transaction, never {@code null}
     */
    @ProtoField(number = 3, required = true)
    public String getChangeTime(Transaction transaction) {
        return transaction.getChangeTime().toString();
    }

    /**
     * A ProtoStream handler to extract the {@code events} field from the {@link Transaction}.
     *
     * @param transaction the transaction instance, must not be {@code null}
     * @return list of events within the transaction
     */
    @ProtoField(number = 4)
    public List<LogMinerEvent> getEvents(Transaction transaction) {
        return transaction.getEvents();
    }

    @ProtoField(number = 5)
    public String getUserName(Transaction transaction) {
        return transaction.getUserName();
    }

    /**
     * A ProtoStream handler to extract the {@code eventIds} field from the {@link Transaction}.
     *
     * @param transaction the transaction instance, must not be {@code null}
     * @return the number of events in the transaction
     */
    @ProtoField(number = 6, defaultValue = "0")
    public int getNumberOfEvents(Transaction transaction) {
        return transaction.getNumberOfEvents();
    }
}
