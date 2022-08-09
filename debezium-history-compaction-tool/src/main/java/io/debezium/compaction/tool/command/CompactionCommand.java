/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.compaction.tool.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.compaction.tool.service.compaction.DatabaseHistoryListener;
import io.debezium.compaction.tool.service.compaction.KafkaDatabaseHistoryCompaction;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.connector.mysql.MySqlReadOnlyIncrementalSnapshotContext;
import io.debezium.connector.mysql.SourceInfo;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Tables;
import io.quarkus.picocli.runtime.annotations.TopCommand;

import picocli.CommandLine;

/**
 * @author Jatinder
 */
@TopCommand
@CommandLine.Command(name = "compaction", mixinStandardHelpOptions = true, description = "Starts the history compaction process", subcommands = {
        CommandLine.HelpCommand.class,
})
public class CompactionCommand implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionCommand.class);

    @CommandLine.Option(names = { "-s", "--bootstrap-server" }, required = true, description = "The bootstrap server address")
    String bootstrapServers;

    @CommandLine.Option(names = { "-t", "--history-topic" }, required = true, description = "The offset topic name for compaction history needs to be stored")
    String historyTopic;

    @CommandLine.Option(names = { "-c", "--connector-name" }, required = true, description = "The debezium connector name for which compaction should be run")
    String connectorName;

    @CommandLine.Option(names = { "-ht",
            "--compacted-history-topic" }, required = true, description = "The new history topic name for compaction history needs to be stored")
    String compactedHistoryTopic;

    @Override
    public void run() {

        LOGGER.info("compaction command executed {} {} {} {}", connectorName, compactedHistoryTopic, bootstrapServers, historyTopic);
        KafkaDatabaseHistoryCompaction databaseHistoryCompaction = new KafkaDatabaseHistoryCompaction(bootstrapServers, historyTopic, compactedHistoryTopic);

        Configuration dbHistoryConfig = Configuration.create().build();

        Configuration.create().build();

        // configure
        databaseHistoryCompaction.configure(dbHistoryConfig, null, DatabaseHistoryListener.NOOP, false);

        // start
        databaseHistoryCompaction.start();

        MySqlPartition source = new MySqlPartition("dbserver1", "my-db-history");
        Configuration config = Configuration.empty()
                .edit()
                .with(RelationalDatabaseConnectorConfig.SERVER_NAME, "dbserver1").build();

        MySqlOffsetContext position = new MySqlOffsetContext(false, true, new TransactionContext(), new MySqlReadOnlyIncrementalSnapshotContext<>(),
                new SourceInfo(new MySqlConnectorConfig(config)));
        Offsets<MySqlPartition, MySqlOffsetContext> offsets = Offsets.of(source, position);

        // record
        databaseHistoryCompaction.record(offsets.getTheOnlyPartition().getSourcePartition(), offsets.getTheOnlyOffset().getOffset(), new Tables(),
                new MySqlAntlrDdlParser());

    }
}
