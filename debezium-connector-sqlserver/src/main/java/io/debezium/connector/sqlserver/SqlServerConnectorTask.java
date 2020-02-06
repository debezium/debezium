/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Collections;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.common.ChangeEventSourceBasedTask;
import io.debezium.pipeline.ChangeEventSourceFacade;
import io.debezium.pipeline.spi.OffsetContext;

/**
 * The main task executing streaming from SQL Server.
 * Responsible for lifecycle management the streaming code.
 *
 * @author Jiri Pechanec
 *
 */
public class SqlServerConnectorTask extends ChangeEventSourceBasedTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerConnectorTask.class);

	@Override
	public String version() {
		return Module.version();
	}

	@Override
	protected ChangeEventSourceFacade createEventSourceFacade(Configuration config) {
		return new SqlServerChangeEventSourceFacade(config, loader -> getPreviousOffset(loader));
	}

	/**
	 * Loads the connector's persistent offset (if present) via the given loader.
	 */
	@Override
	protected OffsetContext getPreviousOffset(OffsetContext.Loader loader) {
		Map<String, ?> partition = loader.getPartition();

		Map<String, Object> previousOffset = context.offsetStorageReader()
				.offsets(Collections.singleton(partition))
				.get(partition);

		if (previousOffset != null) {
			OffsetContext offsetContext = loader.load(previousOffset);
			LOGGER.info("Found previous offset {}", offsetContext);
			return offsetContext;
		}
		else {
			return null;
		}
	}

	@Override
	protected Iterable<Field> getAllConfigurationFields() {
		return SqlServerConnectorConfig.ALL_FIELDS;
	}
}
