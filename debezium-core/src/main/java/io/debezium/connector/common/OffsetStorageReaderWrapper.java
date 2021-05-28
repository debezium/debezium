package io.debezium.connector.common;

import java.util.Collection;
import java.util.Map;

public interface OffsetStorageReaderWrapper {

    <T> Map<String, Object> offset(Map<String, T> partition);

    /**
     * <p>
     * Get a set of offsets for the specified partition identifiers. This may be more efficient
     * than calling {@link #offset(Map)} repeatedly.
     * </p>
     * <p>
     * Note that when errors occur, this method omits the associated data and tries to return as
     * many of the requested values as possible. This allows a task that's managing many partitions to
     * still proceed with any available data. Therefore, implementations should take care to check
     * that the data is actually available in the returned response. The only case when an
     * exception will be thrown is if the entire request failed, e.g. because the underlying
     * storage was unavailable.
     * </p>
     *
     * @param partitions set of identifiers for partitions of data
     * @return a map of partition identifiers to decoded offsets
     */
    <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions);
}
