package io.debezium.connector.common;

import java.io.Serializable;
import java.util.Map;

public interface SourceTaskContextWrapper extends Serializable {

    Map<String, String> configs();

    /**
     * Get the OffsetStorageReader for this SourceTask.
     */
    OffsetStorageReaderWrapper offsetStorageReader();
}
