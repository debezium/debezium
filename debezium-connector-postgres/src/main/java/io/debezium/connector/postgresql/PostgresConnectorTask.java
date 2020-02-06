/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.ChangeEventSourceBasedTask;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ChangeEventSourceFacade;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;

/**
 * Kafka connect source task which uses Postgres logical decoding over a streaming replication connection to process DB changes.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresConnectorTask extends ChangeEventSourceBasedTask {

	private PostgresChangeEventSourceFacade postgresChangeEventSourceFacade;

	@Override
	protected ChangeEventSourceFacade createEventSourceFacade(Configuration config) {
		postgresChangeEventSourceFacade = new PostgresChangeEventSourceFacade(config, loader -> getPreviousOffset(loader));
		return postgresChangeEventSourceFacade;
	}

	@Override
	public String version() {
		return Module.version();
	}

	@Override
	protected Iterable<Field> getAllConfigurationFields() {
		return PostgresConnectorConfig.ALL_FIELDS;
	}

	/**
	 * TODO remove this used only in test
	 */
	PostgresTaskContext getTaskContext() {
		return postgresChangeEventSourceFacade.getTaskContext();
	}

}
