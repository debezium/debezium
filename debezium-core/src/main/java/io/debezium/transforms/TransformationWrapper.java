package io.debezium.transforms;

import java.io.Closeable;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

public interface TransformationWrapper<R extends ConnectRecordWrapper<R>> extends Closeable {

    /**
     * Apply transformation to the {@code record} and return another record object (which may be {@code record} itself) or {@code null},
     * corresponding to a map or filter operation respectively.
     * <p>
     * The implementation must be thread-safe.
     */
    R apply(R record);

    /**
     * Configuration specification for this transformation.
     **/
    ConfigDef config();

    void configure(Map<String, ?> configs);
}
