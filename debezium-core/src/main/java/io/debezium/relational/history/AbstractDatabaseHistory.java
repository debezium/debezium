/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;

/**
 * @author Randall Hauch
 *
 */
public abstract class AbstractDatabaseHistory implements DatabaseHistory {

    protected Configuration config;
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected AbstractDatabaseHistory() {
    }

    @Override
    public void configure(Configuration config) {
        this.config = config;
    }

    @Override
    public final void record(Map<String, ?> source, Map<String, ?> position, String databaseName, Tables schema, String ddl) {
        storeRecord(new HistoryRecord(source, position, databaseName, ddl));
    }

    @Override
    public final void recover(Map<String, ?> source, Map<String, ?> position, Tables schema, DdlParser ddlParser) {
        HistoryRecord stopPoint = new HistoryRecord(source, position, null, null);
        recoverRecords(schema,ddlParser,recovered->{
            if (recovered.isAtOrBefore(stopPoint)) {
                ddlParser.setCurrentSchema(recovered.databaseName()); // may be null
                String ddl = recovered.ddl();
                if (ddl != null) {
                    ddlParser.parse(ddl, schema);
                }
            }
        });
    }

    protected abstract void storeRecord(HistoryRecord record);

    protected abstract void recoverRecords(Tables schema, DdlParser ddlParser, Consumer<HistoryRecord> records);
    
    @Override
    public void shutdown() {
        // do nothing
    }
}
