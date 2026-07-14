/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.metrics;

/**
 * Metrics exposed by the JDBC sink connector over JMX.
 * <p>
 * Kafka Connect already exposes record-throughput and batch metrics for every sink task (e.g.
 * {@code sink-record-send-rate}, {@code put-batch-avg-time-ms}). Those stop at the connector's
 * {@code put()} boundary and say nothing about the target database. These metrics intentionally
 * complement them by describing what the sink actually applied to the target: the number of
 * insert/update/upsert/delete/truncate operations, how many records were skipped, and whether the
 * connection to the target database is currently established.
 * <p>
 * Insert, update, and upsert operations are counted according to the connector's configured
 * {@code insert.mode}, which determines the SQL statement that is used for non-delete records.
 *
 */
public interface JdbcSinkConnectorMetricsMXBean {

    String METRIC_TOTAL_NUMBER_OF_WRITES = "TotalNumberOfWrites";
    String METRIC_TOTAL_NUMBER_OF_DELETES = "TotalNumberOfDeletes";
    String METRIC_TOTAL_NUMBER_OF_TRUNCATES = "TotalNumberOfTruncates";
    String METRIC_NUMBER_OF_FILTERED_EVENTS = "TotalNumberOfFilteredEvents";
    String METRIC_TOTAL_NUMBER_OF_TABLES_CREATED = "TotalNumberOfTablesCreated";
    String METRIC_TOTAL_NUMBER_OF_TABLES_ALTERED = "TotalNumberOfTablesAltered";

    /**
     * @return the total number of processed events applied to the target database
     */
    long getTotalNumberOfWrites();

    /**
     * @return the total number of delete operations applied to the target database
     */
    long getTotalNumberOfDeletes();

    /**
     * @return the total number of truncate operations applied to the target database
     */
    long getTotalNumberOfTruncates();

    /**
     * @return the number of records that were skipped without being applied (e.g. unresolvable
     *         table, deletes/truncates disabled, schema-change events)
     */
    long getTotalNumberOfFilteredEvents();

    /**
     * @return the total number of tables the sink created in the target database; only increases
     *         when {@code schema.evolution} is enabled
     */
    long getTotalNumberOfTablesCreated();

    /**
     * @return the total number of tables the sink altered in the target database; only increases
     *         when {@code schema.evolution} is enabled
     */
    long getTotalNumberOfTablesAltered();

}
