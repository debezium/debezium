package io.debezium.connector.postgresql.snapshot;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.spi.OffsetState;
import io.debezium.connector.postgresql.spi.SlotState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Snapshotter class to take snapshot using parallel tasks.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class ParallelSnapshotter extends QueryingSnapshotter {
    private final static Logger LOGGER = LoggerFactory.getLogger(ParallelSnapshotter.class);
    private OffsetState sourceInfo;

    @Override
    public void init(PostgresConnectorConfig config, OffsetState sourceInfo, SlotState slotState) {
        super.init(config, sourceInfo, slotState);
        this.sourceInfo = sourceInfo;

        LOGGER.info("Initialised ParallelSnapshotter for task {}", config.getTaskId());
    }

    @Override
    public boolean shouldStream() {
        return false;
    }

    @Override
    public boolean shouldSnapshot() {
        if (sourceInfo == null) {
            LOGGER.info("Taking parallel snapshot for new datasource");
            return true;
        }
        else if (sourceInfo.snapshotInEffect()) {
            LOGGER.info("Found previous incomplete snapshot");
            return true;
        }
        else {
            LOGGER.info("Previous snapshot completed, no snapshot will be performed");
            return false;
        }
    }
}
