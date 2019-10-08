/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.debezium.annotation.Immutable;
import io.debezium.function.Predicates;

/**
 * An immutable definition of a table's key. By default, the key will be
 * comprised of the primary key column(s). A key can be customized with a
 * {@code KeyMapper}.
 *
 * @author Guillaume Rosauro
 */
@Immutable
public class Key {

    private final Table table;
    private final KeyMapper keyMapper;

    private Key(Table table, KeyMapper keyMapper) {
        this.table = table;
        this.keyMapper = keyMapper;
    }

    /**
     *
     * @return the columns {@link Column} taking part of the key.
     */
    public List<Column> keyColumns() {
        return keyMapper.getKeyKolumns(table);
    }

    public static class Builder {

        private final Table table;
        private KeyMapper keyMapper = IdentityKeyMapper.getInstance();

        public Builder(Table table) {
            this.table = table;
        }

        public Builder customKeyMapper(KeyMapper customKeyMapper) {
            if (customKeyMapper != null) {
                this.keyMapper = customKeyMapper;
            }
            return this;
        }

        public Key build() {
            return new Key(table, keyMapper);
        }
    }

    /**
     * Provides the column(s) that should be used within the message key for a given table.
     */
    @FunctionalInterface
    public static interface KeyMapper {

        /**
         * @param table {@code Table}
         * @return the list of {@code Column}s describing the (message) key of the table
         */
        List<Column> getKeyKolumns(Table table);
    }

    /**
     * Default Key mapper using PK as key.
     */
    private static class IdentityKeyMapper {

        public static KeyMapper getInstance() {
            return (table) -> table.primaryKeyColumns();
        }
    }

    /**
     * Custom Key mapper used to override or defining a custom {@code Key}
     */
    public static class CustomKeyMapper {

        /**
         * Getting an instance with a list of regexp (table:column1,column2) delimited by ';' matching the tables keys.
         * ex: inventory.customers:pk1,pk2;(.*).purchaseorders:pk3,pk4
         *
         * @param fullyQualifiedColumnNames a list of regex
         * @return a new {@code CustomKeyMapper} or null if fullyQualifiedColumnNames is invalid.
         */
        public static KeyMapper getInstance(String fullyQualifiedColumnNames) {
            if (fullyQualifiedColumnNames == null) {
                return null;
            }

            // transform the 'message.key.columns' option into a list of regexp.
            // ex: message.key.columns=inventory.customers:pk1,pk2;(.*).purchaseorders:pk3,pk4
            // will become => [inventory.customers.pk1,inventory.customers.pk2,(.*).purchaseorders.pk3,(.*).purchaseorders.pk4]
            // then joining those values
            String regexes = Arrays.asList(fullyQualifiedColumnNames.split(";"))
                    .stream()
                    .map(s -> s.split(":"))
                    .collect(
                            ArrayList<String>::new,
                            (m, p) -> Arrays.asList(p[1].split(",")).forEach(c -> m.add(p[0] + "." + c)),
                            ArrayList::addAll)
                    .stream()
                    .collect(Collectors.joining(","));

            Predicate<ColumnId> delegate = Predicates.includes(regexes, ColumnId::toString);

            return (table) -> {
                return table.columns()
                        .stream()
                        .filter(c -> {
                            final TableId tableId = table.id();
                            return delegate.test(new ColumnId(tableId.catalog(), tableId.schema(), tableId.table(), c.name()));
                        })
                        .collect(Collectors.toList());
            };
        }
    }
}
