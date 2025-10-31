/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.spi;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.logminer.buffered.BufferedLogMinerStreamingChangeEventSource;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.SignalAction;
import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Strings;

/**
 * Signal action to drop (abandon) a transaction from the Oracle LogMiner buffer.
 *
 * @author Debezium Community
 */
public class DropTransactionAction<P extends Partition> implements SignalAction<P> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DropTransactionAction.class);

    public static final String NAME = "drop-transaction";
    private static final String FIELD_TRANSACTION_ID = "transaction-id";

    private final ChangeEventSourceCoordinator<P, ?> changeEventSourceCoordinator;

    public DropTransactionAction(ChangeEventSourceCoordinator<P, ?> changeEventSourceCoordinator) {
        this.changeEventSourceCoordinator = changeEventSourceCoordinator;
    }

    @Override
    public boolean arrived(SignalPayload<P> signalPayload) throws InterruptedException {
        LOGGER.info("Processing {} signal: {}", NAME, signalPayload.id);

        if (!(signalPayload.partition instanceof OraclePartition)) {
            throw new DebeziumException(
                    String.format("Signal '%s' with id '%s' is only supported by Oracle connector, but was sent to connector handling partition type: %s",
                            NAME, signalPayload.id, signalPayload.partition.getClass().getSimpleName()));
        }

        final String transactionId = signalPayload.data.getString(FIELD_TRANSACTION_ID);
        if (Strings.isNullOrEmpty(transactionId)) {
            LOGGER.warn("Drop transaction signal '{}' has arrived but the required field '{}' is missing or empty from data",
                    signalPayload.id, FIELD_TRANSACTION_ID);
            return false;
        }

        // Use new coordinator accessor that enables connector-specific signal actions to access streaming sources
        final BufferedLogMinerStreamingChangeEventSource source = getStreamingSource();
        if (source == null) {
            LOGGER.warn("Cannot process drop transaction signal '{}' - streaming source is not available", signalPayload.id);
            return false;
        }

        final String txId = transactionId.trim().toLowerCase();
        LOGGER.info("Attempting to drop transaction '{}' from Oracle LogMiner buffer as requested by signal '{}'", txId, signalPayload.id);

        final boolean success = source.abandonTransactionById(txId);

        if (success) {
            LOGGER.info("Successfully dropped transaction '{}' from Oracle LogMiner buffer via signal '{}'", txId, signalPayload.id);
        }
        else {
            LOGGER.warn("Transaction '{}' was not found in Oracle LogMiner buffer or could not be dropped via signal '{}'", txId, signalPayload.id);
        }

        return success;
    }

    /**
     * Retrieves the streaming source from the coordinator using reflection.
     * The streamingSource field is protected in ChangeEventSourceCoordinator, so reflection is required
     * for connector-specific signal actions to access connector-specific streaming sources.
     *
     * @return the BufferedLogMinerStreamingChangeEventSource, or null if not available or wrong type
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
