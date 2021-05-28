package io.debezium.connector.common;

import java.util.Map;

public interface SourceTaskContextWrapper {

    Map<String, String> configs();

    /**
     * Get the OffsetStorageReader for this SourceTask.
     */
    OffsetStorageReaderWrapper offsetStorageReader();
}
