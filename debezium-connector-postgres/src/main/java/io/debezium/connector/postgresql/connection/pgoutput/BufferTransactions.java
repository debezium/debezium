/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection.pgoutput;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.connection.ReplicationStream;

/**
 * Manages buffering of replication messages grouped by transaction and sub-transaction IDs.
 * <p>
 * This class allows storing replication messages within transactions and their sub-transactions,
 * supporting operations like beginning a transaction, adding messages, committing, flushing, and discarding.
 * </p>
 * <p>
 * Messages are buffered in memory until a transaction commit triggers processing.
 * </p>
 *
 * Usage typically involves buffering messages during replication and then flushing them upon commit.
 *
 * Thread-safety: This class is not thread-safe and should be externally synchronized if used in concurrent contexts.
 *
 * @author Pranav Tiwari
 */
public class BufferTransactions {

    /**
     * Map of transaction IDs to their corresponding sub-transactions,
     * which map sub-transaction IDs to a list of replication messages.
     */
    private final Map<Long, LinkedHashMap<Long, List<ReplicationMessage>>> bufferedTransactions;

    /**
     * Constructs a new, empty buffer for transactions.
     */
    public BufferTransactions() {
        this.bufferedTransactions = new HashMap<>();
    }

    /**
     * Begins a new transaction by buffering the initial replication message under
     * the specified transaction and sub-transaction IDs.
     *
     * @param transactionId the ID of the transaction to begin
     * @param subTransactionId the ID of the sub-transaction to begin within the transaction
     * @param message the initial replication message to buffer
     */
    public void beginMessage(Long transactionId, Long subTransactionId, ReplicationMessage message) {
        List<ReplicationMessage> messages = new ArrayList<>();
        messages.add(message);
        LinkedHashMap<Long, List<ReplicationMessage>> subTransactions = new LinkedHashMap<>();
        subTransactions.put(subTransactionId, messages);
        this.bufferedTransactions.put(transactionId, subTransactions);
    }

    /**
     * Commits a sub-transaction by buffering the commit replication message
     * under the specified transaction and sub-transaction IDs.
     * <p>
     * This assumes the transaction and sub-transaction already exist in the buffer.
     * </p>
     *
     * @param transactionId the ID of the transaction being committed
     * @param subTransactionId the ID of the sub-transaction being committed
     * @param message the commit replication message to buffer
     */
    public void commitMessage(Long transactionId, Long subTransactionId, ReplicationMessage message) {
        List<ReplicationMessage> messages = new ArrayList<>();
        messages.add(message);
        LinkedHashMap<Long, List<ReplicationMessage>> subTransactions = this.bufferedTransactions.get(transactionId);
        subTransactions.put(subTransactionId, messages);
    }

    /**
     * Adds a replication message to the buffer under the specified transaction
     * and sub-transaction IDs.
     * <p>
     * If the sub-transaction does not exist, it will be created.
     * </p>
     *
     * @param transactionId the ID of the transaction
     * @param subTransactionId the ID of the sub-transaction
     * @param message the replication message to add to the buffer
     */
    public void addToBuffer(Long transactionId, Long subTransactionId, ReplicationMessage message) {
        LinkedHashMap<Long, List<ReplicationMessage>> subTransactions = bufferedTransactions.get(transactionId);

        subTransactions.computeIfAbsent(subTransactionId, id -> new ArrayList<>())
                .add(message);
    }

    /**
     * Processes and flushes all buffered replication messages for a given transaction
     * by invoking the provided message processor on each message.
     * <p>
     * After flushing, the buffered transaction is removed from the buffer.
     * </p>
     *
     * @param transactionId the ID of the transaction to flush
     * @param processor the callback processor to handle replication messages
     * @throws SQLException if processing encounters a database error
     * @throws InterruptedException if the processing thread is interrupted
     */
    public void flushOnCommit(Long transactionId, ReplicationStream.ReplicationMessageProcessor processor)
            throws SQLException, InterruptedException {

        LinkedHashMap<Long, List<ReplicationMessage>> subTransactions = bufferedTransactions.get(transactionId);

        for (List<ReplicationMessage> messages : subTransactions.values()) {
            for (ReplicationMessage message : messages) {
                processor.process(message, transactionId);
            }
        }

        bufferedTransactions.remove(transactionId);
    }

    /**
     * Discards the buffered data for a specific sub-transaction within a transaction.
     * <p>
     * If the transaction ID equals the sub-transaction ID, the entire transaction is discarded.
     * Otherwise, only the specified sub-transaction's buffered messages are removed.
     * </p>
     *
     * @param transactionId the ID of the transaction containing the sub-transaction
     * @param subTransactionId the ID of the sub-transaction to discard
     */
    public void discardSubTransaction(Long transactionId, Long subTransactionId) {
        if (transactionId.equals(subTransactionId)) {
            bufferedTransactions.remove(transactionId);
        }
        else {
            Map<Long, List<ReplicationMessage>> subTransactions = bufferedTransactions.get(transactionId);
            if (subTransactions != null) {
                subTransactions.remove(subTransactionId);
            }
        }
    }
}
