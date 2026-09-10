/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.spi;

import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.logminer.buffered.BufferedLogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.logminer.buffered.PendingTransaction;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.SignalAction;
import io.debezium.pipeline.spi.Partition;

/**
 * Signal action that logs the details of every active and deferred transaction currently pending
 * in the Oracle LogMiner buffer, ordered from oldest to newest, giving an on-demand view of the
 * transactions that influence the connector's mining window.
 * <p>
 * The buffer is only safe to read from the streaming thread, so this action requests
 * {@link #isSynchronous() synchronous} invocation and the mining loop executes it at its next
 * safe point rather than on the thread that delivered the signal.
 *
 * @author Chris Cranford
 */
public class LogPendingTransactionsAction<P extends Partition> implements SignalAction<P> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogPendingTransactionsAction.class);

    public static final String NAME = "log-pending-transactions";

    private final ChangeEventSourceCoordinator<P, ?> changeEventSourceCoordinator;

    public LogPendingTransactionsAction(ChangeEventSourceCoordinator<P, ?> changeEventSourceCoordinator) {
        this.changeEventSourceCoordinator = changeEventSourceCoordinator;
    }

    @Override
    public boolean isSynchronous() {
        return true;
    }

    @Override
    public boolean arrived(SignalPayload<P> signalPayload) throws InterruptedException {
        LOGGER.info("Processing {} signal: {}", NAME, signalPayload.id);

        if (!(signalPayload.partition instanceof OraclePartition)) {
            throw new DebeziumException(
                    String.format("Signal '%s' with id '%s' is only supported by Oracle connector, but was sent to connector handling partition type: %s",
                            NAME, signalPayload.id, signalPayload.partition.getClass().getSimpleName()));
        }

        final BufferedLogMinerStreamingChangeEventSource source = getStreamingSource();
        if (source == null) {
            LOGGER.warn("Cannot process {} signal '{}' - streaming source is not available", NAME, signalPayload.id);
            return false;
        }

        logPendingTransactions(signalPayload.id, source.getPendingTransactions());
        return true;
    }

    private static void logPendingTransactions(String signalId, List<PendingTransaction> transactions) {
        LOGGER.info("Pending transactions in Oracle LogMiner buffer as requested by signal '{}': {} total ({} active, {} deferred)",
                signalId, transactions.size(),
                transactions.stream().filter(t -> !t.deferred()).count(),
                transactions.stream().filter(PendingTransaction::deferred).count());

        for (PendingTransaction transaction : transactions) {
            if (transaction.deferred()) {
                LOGGER.info("Deferred transaction {}: startScn={}, changeTime={}, userName={}, clientId={}, redoThread={}",
                        transaction.transactionId(), transaction.startScn(), transaction.changeTime(),
                        transaction.userName(), transaction.clientId(), transaction.redoThreadId());
            }
            else {
                LOGGER.info("Active transaction {}: startScn={}, changeTime={}, userName={}, clientId={}, redoThread={}, events={}",
                        transaction.transactionId(), transaction.startScn(), transaction.changeTime(),
                        transaction.userName(), transaction.clientId(), transaction.redoThreadId(),
                        transaction.eventCount());
            }
        }
    }

    /**
     * Retrieves the buffered LogMiner streaming source from the coordinator.
     *
     * @return the BufferedLogMinerStreamingChangeEventSource, or null if no streaming source is available
     * @throws DebeziumException if the streaming source is not a BufferedLogMinerStreamingChangeEventSource
     */
    private BufferedLogMinerStreamingChangeEventSource getStreamingSource() {
        final Optional<?> source = changeEventSourceCoordinator.getStreamingSource();
        if (source.isEmpty()) {
            return null;
        }

        return source.filter(BufferedLogMinerStreamingChangeEventSource.class::isInstance)
                .map(BufferedLogMinerStreamingChangeEventSource.class::cast)
                .orElseThrow(() -> new DebeziumException(
                        String.format("Signal '%s' is only supported by BufferedLogMinerStreamingChangeEventSource, but found %s",
                                NAME, source.get().getClass().getSimpleName())));
    }
}
