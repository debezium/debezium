package io.debezium.connector.postgresql.connection.pgoutput;

import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.connection.ReplicationStream;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class BufferTransactions {
    private final Map<Long, LinkedHashMap<Long, List<ReplicationMessage>>> bufferedTransactions;
    public BufferTransactions() {
        this.bufferedTransactions = new HashMap<>();
    }

    public void beginMessage(Long transactionId, Long subTransactionId, ReplicationMessage message) {
        List<ReplicationMessage> messages = new ArrayList<>();
        messages.add(message);
        LinkedHashMap<Long, List<ReplicationMessage>> subTransactions = new LinkedHashMap<>();
        subTransactions.put(subTransactionId, messages);
        this.bufferedTransactions.put(transactionId, subTransactions);
    }

    public void commitMessage(Long transactionId, Long subTransactionId, ReplicationMessage message) {
        List<ReplicationMessage> messages = new ArrayList<>();
        messages.add(message);
        LinkedHashMap<Long, List<ReplicationMessage>> subTransactions = this.bufferedTransactions.get(transactionId);
        subTransactions.put(subTransactionId, messages);
    }

    public void addToBuffer(Long transactionId, Long subTransactionId, ReplicationMessage message) {
        LinkedHashMap<Long, List<ReplicationMessage>> subTransactions = bufferedTransactions.get(transactionId);

        subTransactions.computeIfAbsent(subTransactionId, id -> new ArrayList<>())
                .add(message);
    }

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

    public void discardSubTransaction(Long transactionId, Long subTransactionId) {
        if (transactionId.equals(subTransactionId)) {
            bufferedTransactions.remove(transactionId);
        } else {
            Map<Long, List<ReplicationMessage>> subTransactions = bufferedTransactions.get(transactionId);
            subTransactions.remove(subTransactionId);
        }
    }
}
