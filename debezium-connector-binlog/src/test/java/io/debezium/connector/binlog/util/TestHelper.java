/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.util;

import java.lang.reflect.InvocationTargetException;
import java.util.Random;
import java.util.ServiceLoader;

/**
 * @author Chris Cranford
 */
public class TestHelper {

    /**
     * Create an instance of {@link UniqueDatabase}.
     *
     * @param serverName the server name
     * @param databaseName the database name
     * @return an instance of unique database
     */
    public static UniqueDatabase getUniqueDatabase(String serverName, String databaseName) {
        try {
            final UniqueDatabaseProvider provider = getUniqueDatabaseProvider();
            return provider.getUniqueDatabase().getConstructor(String.class, String.class)
                    .newInstance(serverName, databaseName);
        }
        catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Unable to create UniqueDatabase instance", e);
        }
    }

    /**
     * Create an instance of {@link UniqueDatabase}.
     *
     * @param serverName the server name
     * @param databaseName the database name
     * @param sibling database instance
     * @return an instance of unique database
     */
    public static UniqueDatabase getUniqueDatabase(String serverName, String databaseName, UniqueDatabase sibling) {
        try {
            final UniqueDatabaseProvider provider = getUniqueDatabaseProvider();
            return provider.getUniqueDatabase().getConstructor(String.class, String.class, String.class, String.class)
                    .newInstance(serverName, databaseName, sibling.getIdentifier(), null);
        }
        catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Unable to create UniqueDatabase instance", e);
        }
    }

    /**
     * Creates an instance with given Debezium logical name and database name and a set default charset
     *
     * @param serverName logical Debezium server name
     * @param databaseName the name of the database
     * @param charset the character set
     * @return an instance of unique database
     */
    public static UniqueDatabase getUniqueDatabase(String serverName, String databaseName, String charset) {
        try {
            final UniqueDatabaseProvider provider = getUniqueDatabaseProvider();
            return provider.getUniqueDatabase().getConstructor(String.class, String.class, String.class, String.class)
                    .newInstance(serverName, databaseName, Integer.toUnsignedString(new Random().nextInt(), 36), charset);
        }
        catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Unable to create UniqueDatabase instance", e);
        }
    }

    /**
     * Creates an instance with given Debezium logical name and database name and a set default charset
     *
     * @param serverName logical Debezium server name
     * @param databaseName the name of the database
     * @param identifier the identifier
     * @param charset the character set
     * @return an instance of unique database
     */
    public static UniqueDatabase getUniqueDatabase(String serverName, String databaseName, String identifier, String charset) {
        try {
            final UniqueDatabaseProvider provider = getUniqueDatabaseProvider();
            return provider.getUniqueDatabase().getConstructor(String.class, String.class, String.class, String.class)
                    .newInstance(serverName, databaseName, identifier, charset);
        }
        catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Unable to create UniqueDatabase instance", e);
        }
    }

    private static UniqueDatabaseProvider getUniqueDatabaseProvider() {
        final ServiceLoader<UniqueDatabaseProvider> loader = ServiceLoader.load(UniqueDatabaseProvider.class);
        return loader.findFirst().orElseThrow();
    }
}
