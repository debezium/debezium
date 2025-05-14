/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.infinispan.marshalling;

import java.time.Instant;

import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.buffered.infinispan.InfinispanTransaction;

/**
 * An Infinispan ProtoStream adapter for marshalling a {@link InfinispanTransaction} instance.
 *
 * This class defines a factory for creating {@link InfinispanTransaction} instances when hydrating a transaction
 * record from the protocol buffer datastore as well as field handlers to extract values from a given
 * transaction instance for serializing the instance to a protocol buffer stream.
 *
 * @author Chris Cranford
 */
@ProtoAdapter(InfinispanTransaction.class)
public class TransactionAdapter {

    /**
     * A ProtoStream factory that creates a {@link InfinispanTransaction} instance from field values.
     *
     * @param transactionId the transaction identifier
     * @param scn the starting system change number of the transaction
     * @param changeTime the starting time of the transaction
     * @param numberOfEvents the number of events in the transaction
     * @param userName the user name
     * @param redoThreadId the redo thread id
     * @param clientId the client id
     * @return the constructed Transaction instance
     */
    @ProtoFactory
    public InfinispanTransaction factory(String transactionId, String scn, String changeTime, int numberOfEvents, String userName, Integer redoThreadId,
                                         String clientId) {
        return new InfinispanTransaction(transactionId, Scn.valueOf(scn), Instant.parse(changeTime), userName, numberOfEvents, redoThreadId, clientId);
    }

    /**
     * A ProtoStream handler to extract the {@code transactionId} field from the {@link InfinispanTransaction}.
     *
     * @param transaction the transaction instance, must not be {@code null}
     * @return the transaction identifier, never {@code null}
     */
    @ProtoField(number = 1)
    public String getTransactionId(InfinispanTransaction transaction) {
        return transaction.getTransactionId();
    }

    /**
     * A ProtoStream handler to extract the {@code startScn} field from the {@link InfinispanTransaction}.
     *
     * @param transaction the transaction instance, must not be {@code null}
     * @return the starting system change number, never {@code null}
     */
    @ProtoField(number = 2)
    public String getScn(InfinispanTransaction transaction) {
        // We intentionally serialize the Scn class as a string to the protocol buffer datastore
        // and so the factory method also accepts a string parameter and converts the value to a
        // Scn instance during instantiation. This avoids the need for an additional adapter.
        return transaction.getStartScn().toString();
    }

    /**
     * A ProtoStream handler to extract the {@code changeTime} field from the {@link InfinispanTransaction}.
     *
     * @param transaction the transaction instance, must not be {@code null}
     * @return the starting time of the transaction, never {@code null}
     */
    @ProtoField(number = 3)
    public String getChangeTime(InfinispanTransaction transaction) {
        return transaction.getChangeTime().toString();
    }

    /**
     * A ProtoStream handler to extract the {@code eventIds} field from the {@link InfinispanTransaction}.
     *
     * @param transaction the transaction instance, must not be {@code null}
     * @return the number of events in the transaction
     */
    @ProtoField(number = 4, defaultValue = "0")
    public int getNumberOfEvents(InfinispanTransaction transaction) {
        return transaction.getNumberOfEvents();
    }

    /**
     * A ProtoStream handler to extract the {@code userName} field from the {@link InfinispanTransaction}.
     *
     * @param transaction the transaction instance, must not be {@code null}
     * @return the username associated with the transaction
     */
    @ProtoField(number = 5)
    public String getUserName(InfinispanTransaction transaction) {
        return transaction.getUserName();
    }

    /**
     * A ProtoStream handler to extract the {@code redoThreadId} field from the {@link InfinispanTransaction}.
     *
     * @param transaction the transaction instance, must not be {@code null}
     * @return the redo thread id
     */
    @ProtoField(number = 6)
    public Integer getRedoThreadId(InfinispanTransaction transaction) {
        return transaction.getRedoThreadId();
    }

    /**
     * A ProtoStream handler to extract the {@code clientId} field from the {@link InfinispanTransaction}.
     *
     * @param transaction the transaction instance, must not be {@code null}
     * @return the clientId
     */
    @ProtoField(number = 7)
    public String getClientId(InfinispanTransaction transaction) {
        return transaction.getClientId();
    }
}
