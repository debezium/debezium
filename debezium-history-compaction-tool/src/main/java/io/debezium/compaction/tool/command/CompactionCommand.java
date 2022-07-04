/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.compaction.tool.command;

import io.debezium.compaction.tool.service.compaction.KafkaDatabaseHistoryCompaction;
import io.quarkus.picocli.runtime.annotations.TopCommand;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * @author Jatinder
 */
@TopCommand
@CommandLine.Command(name = "compaction", mixinStandardHelpOptions = true, description = "Starts the history compaction process", subcommands = {
        CommandLine.HelpCommand.class,
})
public class CompactionCommand implements Runnable{
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionCommand.class);

    @ConfigProperty(name = "debezium.history.compaction.kafka.bootstrap.servers")
    String bootstrapServers;

    @ConfigProperty(name = "debezium.history.compaction.kafka.offset.topic")
    String offsetTopic;

    @CommandLine.Option(names = { "-n", "--connector.name" }, description = "The debezium connector name for which compaction should be run")
    String connectorName;

    @CommandLine.Option(names = { "-h", "--history.topic" }, description = "The new database history topic name")
    String compactedHistoryTopic;


    @Override
    public void run() {
        LOGGER.info("compaction command executed {} {}", connectorName, compactedHistoryTopic);
         KafkaDatabaseHistoryCompaction databaseHistoryCompaction = new KafkaDatabaseHistoryCompaction(bootstrapServers, offsetTopic, compactedHistoryTopic);
         databaseHistoryCompaction.start();
    }
}
