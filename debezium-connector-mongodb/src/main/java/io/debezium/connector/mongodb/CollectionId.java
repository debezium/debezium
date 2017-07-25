/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.regex.Pattern;

import io.debezium.annotation.Immutable;

/**
 * A simple identifier for collections in a replica set.
 *
 * @author Randall Hauch
 */
@Immutable
public final class CollectionId {

    private static final Pattern IDENTIFIER_SEPARATOR_PATTERN = Pattern.compile("\\.");

    /**
     * Parse the supplied string, extracting the first 3 parts into a Collection.
     *
     * @param str the string representation of the collection identifier; may not be null
     * @return the collection ID, or null if it could not be parsed
     */
    public static CollectionId parse(String str) {
        String[] parts = IDENTIFIER_SEPARATOR_PATTERN.split(str);
        if (parts.length < 3) return null;
        return new CollectionId(parts[0], parts[1], parts[2]);
    }

    private final String replicaSetName;
    private final String dbName;
    private final String name;

    /**
     * Create a new collection identifier.
     *
     * @param replicaSetName the name of the replica set; may not be null
     * @param dbName the name of the database; may not be null
     * @param collectionName the name of the collection; may not be null
     */
    public CollectionId(String replicaSetName, String dbName, String collectionName) {
        this.replicaSetName = replicaSetName;
        this.dbName = dbName;
        this.name = collectionName;
        assert this.replicaSetName != null;
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

    /**
     * Get the name of the replica set in which the collection (and database) exist.
     *
     * @return the replica set name; never null
     */
    public String replicaSetName() {
        return replicaSetName;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj instanceof CollectionId) {
            CollectionId that = (CollectionId) obj;
            return this.replicaSetName.equals(that.replicaSetName) &&
                    this.dbName.equals(that.dbName) && this.name.equals(that.name);
        }
        return false;
    }

    /**
     * Get the namespace of this collection, which is comprised of the {@link #dbName database name} and {@link #name collection
     * name}.
     *
     * @return the namespace for this collection; never null
     */
    public String namespace() {
        return dbName + "." + name;
    }

    @Override
    public String toString() {
        return replicaSetName + "." + dbName + "." + name;
    }
}
