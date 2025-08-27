package io.debezium.connector.postgresql.connection;

import java.util.Optional;

public interface PositionLocator {

    public Optional<Lsn> resumeFromLsn(Lsn currentLsn, ReplicationMessage message, Long beginMessageTransactionId);
    public boolean skipMessage(Lsn lsn, Long beginMessageTransactionId);
    public void enableFiltering();
    public boolean searchingEnabled();
    public Lsn getLastEventStoredLsn();
    public Lsn getLastCommitStoredLsn();
}
