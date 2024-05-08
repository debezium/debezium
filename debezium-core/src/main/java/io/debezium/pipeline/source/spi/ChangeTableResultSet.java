/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.ChangeTable;

/**
 * A wrapper around a JDBC {@link ResultSet} for a change table for processing rows.
 *
 * @param <C> source connector implementation of {@link ChangeTable}
 * @param <T> source connector implementation of transaction log position
 *
 * @author Jiri Pechanec
 * @author Chris Cranford
 */
public abstract class ChangeTableResultSet<C extends ChangeTable, T extends Comparable<T>> {

    private final static Logger LOGGER = LoggerFactory.getLogger(ChangeTableResultSet.class);

    private final C changeTable;
    private final ResultSet resultSet;
    private final int columnDataOffset;
    private boolean completed = false;
    private T currentChangePosition;
    private T previousChangePosition;

    public ChangeTableResultSet(C changeTable, ResultSet resultSet, int columnDataOffset) {
        this.changeTable = changeTable;
        this.resultSet = resultSet;
        this.columnDataOffset = columnDataOffset;
    }

    public C getChangeTable() {
        return changeTable;
    }

    public T getChangePosition() throws SQLException {
        return currentChangePosition;
    }

    protected T getPreviousChangePosition() {
        return previousChangePosition;
    }

    public int getOperation() throws SQLException {
        return getOperation(resultSet);
    }

    public boolean isCurrentPositionSmallerThanPreviousPosition() {
        return (previousChangePosition != null) && previousChangePosition.compareTo(currentChangePosition) > 0;
    }

    public boolean next() throws SQLException {
        completed = !resultSet.next();
        previousChangePosition = currentChangePosition;
        currentChangePosition = getNextChangePosition(resultSet);
        if (completed) {
            LOGGER.trace("Closing result set of change tables for table {}", changeTable);
            resultSet.close();
        }
        return !completed;
    }

    /**
     * Get the column data from the source change table's result-set
     */
    public Object[] getData() throws SQLException {
        final int dataColumnCount = resultSet.getMetaData().getColumnCount() - (columnDataOffset - 1);
        final Object[] data = new Object[dataColumnCount];
        for (int i = 0; i < dataColumnCount; ++i) {
            data[i] = getColumnData(resultSet, columnDataOffset + i);
        }
        return data;
    }

    /**
     * Get the value of a specific column in the result-set.
     */
    protected Object getColumnData(ResultSet resultSet, int columnIndex) throws SQLException {
        return resultSet.getObject(columnIndex);
    }

    public boolean isCompleted() {
        return completed;
    }

    public int compareTo(ChangeTableResultSet<C, T> other) throws SQLException {
        return getChangePosition().compareTo(other.getChangePosition());
    }

    @Override
    public String toString() {
        return "ChangeTableResultSet{" +
                "changeTable=" + changeTable +
                ", resultSet=" + resultSet +
                ", completed=" + completed +
                ", currentChangePosition=" + currentChangePosition +
                '}';
    }

    protected abstract int getOperation(ResultSet resultSet) throws SQLException;

    protected abstract T getNextChangePosition(ResultSet resultSet) throws SQLException;
}
