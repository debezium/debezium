/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Collections;
import java.util.List;

import io.debezium.annotation.Immutable;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

/**
 * A simple identifier for collections in a replica set.
 *
 * @author Randall Hauch
 */
@Immutable
public final class CollectionId implements DataCollectionId {

    /**
     * Parse the supplied {@code <database_name>.<collection_name>} string.
     * The {@code collection_name} can also contain dots in its value.
     *
     * @param str the string representation of the collection identifier; may not be null
     * @return the collection ID, or null if it could not be parsed
     */
    public static CollectionId parse(String str) {
        final int dotPosition = str.indexOf('.');
        if (dotPosition == -1 || (dotPosition + 1) == str.length() || dotPosition == 0) {
            return null;
        }

        return new CollectionId(str.substring(0, dotPosition), str.substring(dotPosition + 1));
    }

    private final String dbName;
    private final String name;

    /**
     * Create a new collection identifier.
     *
     * @param dbName the name of the database; may not be null
     * @param collectionName the name of the collection; may not be null
     */
    public CollectionId(String dbName, String collectionName) {
        this.dbName = dbName;
        this.name = collectionName;
        assert this.dbName != null;
        assert this.name != null;
    }

    /**
     * Get the name of the collection.
     *
     * @return the collection's name; never null
     */
    public String name() {
        return name;
    }

    /**
     * Get the name of the database in which the collection exists.
     *
     * @return the database name; never null
     */
    public String dbName() {
        return dbName;
    }

    @Override
    public String identifier() {
        return dbName + "." + name;
    }

    @Override
    public List<String> parts() {
        return Collect.arrayListOf(dbName, name);
    }

    @Override
    public List<String> databaseParts() {
        return Collect.arrayListOf(dbName, name);
    }

    @Override
    public List<String> schemaParts() {
        return Collections.emptyList();
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof CollectionId) {
            CollectionId that = (CollectionId) obj;
            return this.dbName.equals(that.dbName) && this.name.equals(that.name);
        }
        return false;
    }

    /**
     * Get the namespace of this collection, which is composed of the {@link #dbName database name} and {@link #name collection
     * name}.
     *
     * @return the namespace for this collection; never null
     */
    public String namespace() {
        return identifier();
    }

    @Override
    public String toString() {
        return identifier();
    }
}
