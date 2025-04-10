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

import com.google.re2j.Pattern;

import io.debezium.annotation.Immutable;
import io.debezium.function.Predicates;
import io.debezium.relational.Selectors.TableIdToStringMapper;

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
    public interface KeyMapper {

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
            return Table::primaryKeyColumns;
        }
    }

    /**
     * Custom Key mapper used to override or defining a custom {@code Key}
     */
    public static class CustomKeyMapper {

        /**
         * Pattern for defining the PK columns of a given table, in the form of "table:column1(,column2,...)",
         * optionally with leading/trailing whitespace.
         */
        public static final Pattern MSG_KEY_COLUMNS_PATTERN = Pattern.compile("^\\s*([^\\s:]+):([^:\\s]+)\\s*$");

        public static final Pattern PATTERN_SPLIT = Pattern.compile(";");
        private static final Pattern TABLE_SPLIT = Pattern.compile(":");
        private static final Pattern COLUMN_SPLIT = Pattern.compile(",");

        /**
         * Getting an instance with a list of regexp (table:column1,column2) delimited by ';' matching the tables keys.
         * ex: inventory.customers:pk1,pk2;(.*).purchaseorders:pk3,pk4
         *
         * @param fullyQualifiedColumnNames a list of regex
         * @param tableIdMapper mapper of tableIds to a String
         * @return a new {@code CustomKeyMapper} or null if fullyQualifiedColumnNames is invalid.
         */
        public static KeyMapper getInstance(String fullyQualifiedColumnNames, TableIdToStringMapper tableIdMapper) {
            if (fullyQualifiedColumnNames == null) {
                return null;
            }

            // transform the 'message.key.columns' option into a list of regexp.
            // ex: message.key.columns=inventory.customers:pk1,pk2;(.*).purchaseorders:pk3,pk4
            // will become => [inventory.customers.pk1,inventory.customers.pk2,(.*).purchaseorders.pk3,(.*).purchaseorders.pk4]
            // then joining those values
            List<Predicate<ColumnId>> predicates = new ArrayList<>(Arrays.stream(PATTERN_SPLIT.split(fullyQualifiedColumnNames))
                    .map(TABLE_SPLIT::split)
                    .collect(
                            ArrayList<String>::new,
                            (m, p) -> Arrays.asList(COLUMN_SPLIT.split(p[1])).forEach(c -> m.add(p[0] + "." + c)),
                            ArrayList::addAll))
                    .stream()
                    .map(regex -> Predicates.includes(regex, ColumnId::toString))
                    .collect(Collectors.toList());

            return (table) -> {
                List<Column> candidates = new ArrayList<>();
                for (Predicate<ColumnId> predicate : predicates) {

                    candidates.addAll(
                            table.columns()
                                    .stream()
                                    .filter(c -> matchColumn(tableIdMapper, table, predicate, c))
                                    .collect(Collectors.toList()));
                }

                return candidates.isEmpty() ? table.primaryKeyColumns() : candidates;
            };
        }
    }

    private static boolean matchColumn(TableIdToStringMapper tableIdMapper, Table table, Predicate<ColumnId> predicate, Column c) {

        final TableId tableId = table.id();
        if (tableIdMapper == null) {
            return predicate.test(new ColumnId(tableId.catalog(), tableId.schema(), tableId.table(), c.name()));
        }
        return predicate.test(new ColumnId(tableId.catalog(), tableId.schema(), tableId.table(), c.name()))
                || predicate.test(
                        new ColumnId(new TableId(tableId.catalog(), tableId.schema(), tableId.table(), tableIdMapper), c.name()));
    }
}
