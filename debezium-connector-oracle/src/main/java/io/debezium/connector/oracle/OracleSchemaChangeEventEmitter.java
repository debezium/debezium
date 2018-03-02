package io.debezium.connector.oracle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import oracle.streams.DDLLCR;

public class OracleSchemaChangeEventEmitter implements SchemaChangeEventEmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleSchemaChangeEventEmitter.class);

    private final OracleOffsetContext offsetContext;
    private final TableId tableId;
    private final DDLLCR ddlLcr;

    public OracleSchemaChangeEventEmitter(OracleOffsetContext offsetContext, TableId tableId, DDLLCR ddlLcr) {
        this.offsetContext = offsetContext;
        this.tableId = tableId;
        this.ddlLcr = ddlLcr;
    }

    @Override
    public void emitSchemaChangeEvent(Receiver receiver) throws InterruptedException {
        SchemaChangeEventType eventType = getSchemaChangeEventType();
        if (eventType != null) {
            Table table = new OracleDdlParser().parseCreateTable(tableId, ddlLcr.getDDLText());
            receiver.schemaChangeEvent(new SchemaChangeEvent(offsetContext.getPartition(), offsetContext.getOffset(), ddlLcr.getSourceDatabaseName(), ddlLcr.getDDLText(), table, eventType, false));
        }
    }

    private SchemaChangeEventType getSchemaChangeEventType() {
        switch(ddlLcr.getCommandType()) {
            case "CREATE TABLE": return SchemaChangeEventType.CREATE;
            case "ALTER TABLE": throw new UnsupportedOperationException("ALTER TABLE not yet implemented");
            case "DROP TABLE": throw new UnsupportedOperationException("DROP TABLE not yet implemented");
            default:
                LOGGER.debug("Ignoring DDL event of type {}", ddlLcr.getCommandType());
                return null;
        }
    }
}
