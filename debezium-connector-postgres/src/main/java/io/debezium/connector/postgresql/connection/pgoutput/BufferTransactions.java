package io.debezium.connector.postgresql.connection.pgoutput;

import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.connection.ReplicationStream;

import java.sql.SQLException;
import java.time.Instant;
import java.util.*;

public class BufferTransactions {
    private final Map<Long, LinkedHashMap<Long, List<ReplicationMessage>>> bufferedTransactions;
    public BufferTransactions() {
        this.bufferedTransactions = new HashMap<>();
    }
    public void addToBuffer(Long transactionId, Long subTransactionId, ReplicationMessage message) {
        LinkedHashMap<Long, List<ReplicationMessage>> subTransactions =
                bufferedTransactions.computeIfAbsent(transactionId, id -> new LinkedHashMap<>());

        subTransactions.computeIfAbsent(subTransactionId, id -> new ArrayList<>())
                .add(message);
    }
    public void flushOnCommit(Long transactionId, ReplicationStream.ReplicationMessageProcessor processor)
            throws SQLException, InterruptedException {

        LinkedHashMap<Long, List<ReplicationMessage>> subTransactions = bufferedTransactions.get(transactionId);
        /*Optional<ReplicationMessage> maybeCommit = subTransactions.get(transactionId).stream()
                .filter(replicationMessage -> replicationMessage.getOperation().equals(ReplicationMessage.Operation.COMMIT))
                .findFirst();

        Instant commitTimeStamp = maybeCommit
                .map(ReplicationMessage::getCommitTime)
                .orElse(Instant.now());*/


        for (List<ReplicationMessage> messages : subTransactions.values()) {
            for (ReplicationMessage message : messages) {
                processor.process(message);
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
