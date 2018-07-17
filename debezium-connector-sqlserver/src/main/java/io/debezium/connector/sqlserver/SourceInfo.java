package io.debezium.connector.sqlserver;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.AbstractSourceInfo;

/**
 * Coordinates from the database log to restart streaming from. Maps to {@code source} field in envelope and
 * to connector offsets.
 *
 * @author Jiri Pechanec
 *
 */
public class SourceInfo extends AbstractSourceInfo {

    public static final String SERVER_NAME_KEY = "name";
    public static final String LOG_TIMESTAMP_KEY = "ts_ms";
    public static final String CHANGE_LSN_KEY = "change_lsn";
    public static final String SNAPSHOT_KEY = "snapshot";

    public static final Schema SCHEMA = schemaBuilder()
            .name("io.debezium.connector.sqlserver.Source")
            .field(SERVER_NAME_KEY, Schema.STRING_SCHEMA)
            .field(LOG_TIMESTAMP_KEY, Schema.OPTIONAL_INT64_SCHEMA)
            .field(CHANGE_LSN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(SNAPSHOT_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .build();

    private final String serverName;
    private Lsn changeLsn;
    private Instant sourceTime;

    protected SourceInfo(String serverName) {
        super(Module.version());
        this.serverName = serverName;
    }

    public void setChangeLsn(Lsn lsn) {
        changeLsn = lsn;
    }

    public Lsn getChangeLsn() {
        return changeLsn;
    }

    public void setSourceTime(Instant instant) {
        sourceTime = instant;
    }

    @Override
    protected Schema schema() {
        return SCHEMA;
    }

    @Override
    public Struct struct() {
        return super.struct()
                .put(SERVER_NAME_KEY, serverName)
                .put(LOG_TIMESTAMP_KEY, sourceTime == null ? null : sourceTime.toEpochMilli())
                .put(CHANGE_LSN_KEY, changeLsn.toString())
                .put(SNAPSHOT_KEY, false);
    }
}
