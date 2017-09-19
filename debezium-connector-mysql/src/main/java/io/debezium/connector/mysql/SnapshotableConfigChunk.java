package io.debezium.connector.mysql;

import io.debezium.config.Configuration;
import io.debezium.relational.Selectors;
import io.debezium.relational.TableId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A class representing a single, independently snapshotable config chunk.
 */
public abstract class SnapshotableConfigChunk {

    protected SourceInfo sourceInfo;
    protected Configuration config;

    public SnapshotableConfigChunk(Configuration config, SourceInfo sourceInfo) {
        this.sourceInfo = sourceInfo;
        this.config = config;
    }

    /**
     * Creates and returns a reader to snapshot the given chunk.
     * @return a {@link Reader} for the given chunk of the config.
     */
    public abstract Reader createSnapshotReader();

    /**
     * Mark this chunk as resolved in {@link SourceInfo}
     */
    public abstract void markChunkResolved();

    /**
     * A chunk representing a new database being added to the database whitelist configuration.
     * If multiple databases are added to the whitelist, they can be resolved in separate snapshots.
     */
    public static class SingleDbWhitelistChunk extends SnapshotableConfigChunk {

        private String whitelistedDb;
        private Collection<TableId> blacklistedTables;

        /**
         * @param whitelistedDb the whitelisted database that is the core of this chunk
         * @param blacklistedTables any tables in the whitelisted database that are blacklisted
         * @param config the connector {@link Configuration}
         * @param sourceInfo the {@link SourceInfo}
         */
        public SingleDbWhitelistChunk(String whitelistedDb,
                                      Collection<TableId> blacklistedTables,
                                      Configuration config,
                                      SourceInfo sourceInfo) {
            super(config, sourceInfo);
            this.whitelistedDb = whitelistedDb;
            this.blacklistedTables = blacklistedTables;
        }

        @Override
        public Reader createSnapshotReader() {
            return null;
        }

        @Override
        public void markChunkResolved() {
            if(blacklistedTables == null) {
                sourceInfo.addWhitelistedDatabase(whitelistedDb);
            } else {
                sourceInfo.addWhitelistedDatabase(whitelistedDb,
                                                  blacklistedTables);
            }
        }
    }

    /**
     * A chunk representing an entirely new database blacklist being added to the configuration, where previously there was a database whitelist
     * This change needs to be resolved all at once.
     */
    public static class AllDbBlacklistChunk extends SnapshotableConfigChunk {

        private Collection<String> blacklistedDbs;
        private Collection<TableId> blacklistedTables;

        /**
         * @param blacklistedDbs the new collection of blacklisted databases
         * @param blacklistedTables the set of blacklisted tables
         * @param config the connector {@link Configuration}
         * @param sourceInfo the {@link SourceInfo}
         */
        public AllDbBlacklistChunk(Collection<String> blacklistedDbs,
                                   Collection<TableId> blacklistedTables,
                                   Configuration config,
                                   SourceInfo sourceInfo) {
            super(config, sourceInfo);
            this.blacklistedDbs = blacklistedDbs;
            this.blacklistedTables = blacklistedTables;
        }

        @Override
        public Reader createSnapshotReader() {
            return null;
        }

        @Override
        public void markChunkResolved() {
            if (blacklistedTables == null) {
                sourceInfo.addBlacklistedDatabases(blacklistedDbs);
            } else {
                sourceInfo.addBlacklistedDatabases(blacklistedDbs,
                                                   blacklistedTables);
            }
        }
    }

    /**
     * A chunk representing a new table being added to the table whitelist configuration.
     * If mutliple tables are added to the whitelist, they can be resolved in separate snapshots.
     */
    public static class SingleTableWhitelistChunk extends SnapshotableConfigChunk {

        private TableId whitelistedTableId;

        /**
         * @param whitelistedTableId the single whitelisted {@link TableId} that is the core of this chunk
         * @param config the connector {@link Configuration}
         * @param sourceInfo the {@link SourceInfo}
         */
        public SingleTableWhitelistChunk(TableId whitelistedTableId,
                                         Configuration config,
                                         SourceInfo sourceInfo) {
            super(config, sourceInfo);
            this.whitelistedTableId = whitelistedTableId;
        }

        @Override
        public Reader createSnapshotReader() {
            SnapshotTableReader snapshotTableReader =
                new SnapshotTableReader(whitelistedTableId,
                                        whitelistedTableId + "Snapshot",
                                        new MySqlTaskContext(config)); // should this be a new task context? should all these share the same task context?
            //snapshotTableReader.uponCompletion(); // todo runnable w/ binlog catch up, followed by config section being marked as resolved.
            // todo side note, nothing (I think) is preventing the SnapshotTableReader from setting its OWN uponCompletion. At least the binlog portion.
            // it needs some external knowledge to know if the config needs to be resolved (and it's not dealing with that anyways, so
            // it needs some method to set some extra stuff that needs to happen on completion.)
            return snapshotTableReader;
        }

        @Override
        public void markChunkResolved() {
            sourceInfo.addWhitelistedTable(whitelistedTableId);
        }
    }

    /**
     * A chunk representing an entirely new table blacklist being added to the configuration, where previously there was a table whitelist
     * This change needs to be resolved all at once.
     */
    public static class AllTableBlacklistChunk extends SnapshotableConfigChunk {

        private Collection<TableId> blacklistedTables;

        /**
         * @param blacklistedTables the new collection of blacklisted tables
         * @param config the connector {@link Configuration}
         * @param sourceInfo the {@link SourceInfo}
         */
        public AllTableBlacklistChunk(Collection<TableId> blacklistedTables,
                                      Configuration config,
                                      SourceInfo sourceInfo) {
            super(config, sourceInfo);
            this.blacklistedTables = blacklistedTables;
        }

        @Override
        public Reader createSnapshotReader() {
            return null;
        }

        @Override
        public void markChunkResolved() {
            sourceInfo.addBlacklistedTables(blacklistedTables);
        }
    }

    /**
     * A chunk representing the removal of a blacklisted database from the configuration
     * If multiple databases were removed from the blacklist, they can be resolved separately
     */
    public static class RemovedDbBlacklistChunk extends SnapshotableConfigChunk {

        private String previouslyBlacklistedDb;
        private Collection<TableId> blacklistedTables;

        /**
         * @param previouslyBlacklistedDb the single, previously blacklisted database that forms the core of this chunk
         * @param blacklistedTables any tables in the previously blacklisted database that are (currently) blacklisted
         * @param config the connector {@link Configuration}
         * @param sourceInfo the {@link SourceInfo}
         */
        public RemovedDbBlacklistChunk(String previouslyBlacklistedDb,
                                       Collection<TableId> blacklistedTables,
                                       Configuration config,
                                       SourceInfo sourceInfo) {
            super(config, sourceInfo);
            this.previouslyBlacklistedDb = previouslyBlacklistedDb;
            this.blacklistedTables = blacklistedTables;
        }

        @Override
        public Reader createSnapshotReader() {
            return null;
        }

        @Override
        public void markChunkResolved() {
            if(blacklistedTables == null) {
                sourceInfo.removeBlacklistedDatabase(previouslyBlacklistedDb);
            } else {
                sourceInfo.removeBlacklistedDatabase(previouslyBlacklistedDb,
                                                     blacklistedTables);
            }
        }
    }

    /**
     * A chunk representing the removal of a blacklisted table from the configuration
     * If multiple tables are removed from the blacklist, they can be resolved separately
     */
    public static class RemovedTableBlacklistChunk extends SnapshotableConfigChunk {

        private TableId previouslyBlacklistedTable;

        /**
         * @param previouslyBlacklistedTable the single, previously blacklisted table that forms the core of this chunk
         * @param config the connector {@link Configuration}
         * @param sourceInfo the {@link SourceInfo}
         */
        public RemovedTableBlacklistChunk(TableId previouslyBlacklistedTable,
                                          Configuration config,
                                          SourceInfo sourceInfo) {
            super(config, sourceInfo);
            this.previouslyBlacklistedTable = previouslyBlacklistedTable;
        }

        @Override
        public Reader createSnapshotReader() {
            return null;
        }

        @Override
        public void markChunkResolved() {
            sourceInfo.removeBlacklistedTable(previouslyBlacklistedTable);
        }
    }

    /**
     * Divide the given config information into a collection of individually snapshotable chunks. Auto-resolve
     * any config parts that need no action to be resolved.
     *
     * @param config the connector {@link Configuration}
     * @param sourceInfo the {@link SourceInfo}
     * @return A Collection of {@link SnapshotableConfigChunk}s for everything that needs resolution.
     */
    public static Collection<SnapshotableConfigChunk> divideNewConfigIntoChunks(Configuration config,
                                                                                SourceInfo sourceInfo) {

        Collection<SnapshotableConfigChunk> configChunks = makeDatabaseChunks(config,
                                                                              sourceInfo);

        configChunks.addAll(makeTableChunks(config,
                                            sourceInfo));

        return configChunks;
    }

    /**
     * Create and return Snapshotable Config chunks for any databases that need resolving. Additionally, auto-resolve
     * any databases that do not need any action taken to be resolved (for example, a newly blacklisted database).
     *
     * @param config the connector {@link Configuration}
     * @param sourceInfo the {@link SourceInfo}
     * @return a Collection of {@link SnapshotableConfigChunk}s for databases that need resolution.
     */
    private static Collection<SnapshotableConfigChunk> makeDatabaseChunks(Configuration config,
                                                                          SourceInfo sourceInfo) {
        Collection<SnapshotableConfigChunk> configChunks = new ArrayList<>();

        Collection<String> configDbWhitelist = config.getStrings(MySqlConnectorConfig.DATABASE_WHITELIST, MySqlConnectorConfig.WHITELIST_BLACKLIST_DELIMITER);
        Collection<String> configDbBlacklist = config.getStrings(MySqlConnectorConfig.DATABASE_BLACKLIST, MySqlConnectorConfig.WHITELIST_BLACKLIST_DELIMITER);
        Collection<TableId> configTableBlacklist = config.getElements(MySqlConnectorConfig.TABLE_BLACKLIST,
                                                                      MySqlConnectorConfig.WHITELIST_BLACKLIST_DELIMITER,
                                                                      TableId::parse);

        Collection<String> resolvedDbWhitelist = sourceInfo.getResolvedDatabaseWhitelist();
        Collection<String> resolvedDbBlacklist = sourceInfo.getResolvedDatabaseBlacklist();

        Predicate<String> resolvedDbFilter = Selectors.databaseSelector()
                                                      .includeDatabases(resolvedDbWhitelist)
                                                      .excludeDatabases(resolvedDbBlacklist)
                                                      .build();

        if (configDbWhitelist != null) {
            for (String whitelistedDb : configDbWhitelist) {
                if (!resolvedDbFilter.test(whitelistedDb)) {
                    // this database has not yet been resolved.
                    SingleDbWhitelistChunk chunk = new SingleDbWhitelistChunk(whitelistedDb,
                                                                              getBlacklistedTables(whitelistedDb,
                                                                                                   configTableBlacklist),
                        config, sourceInfo);
                    configChunks.add(chunk);
                } else {
                    // if it is resolved, make sure this db is marked as resolved.
                    sourceInfo.addWhitelistedDatabase(whitelistedDb);
                }
            }
        } else if (configDbBlacklist != null) {
            if (!resolvedDbWhitelist.isEmpty()) {
                // totally new blacklist; has to be resolved in one complete sweep.
                AllDbBlacklistChunk chunk = new AllDbBlacklistChunk(configDbBlacklist, configTableBlacklist, config, sourceInfo);
                configChunks.add(chunk);
            } else if (!resolvedDbBlacklist.isEmpty()) {
                // maybe the blacklist has been modified.
                Collection<String> removedDatabases = listRemove(resolvedDbBlacklist, configDbBlacklist);
                if (!removedDatabases.isEmpty()) {
                    // some databases have been removed from the blacklist. Each removal can be resolved individually.
                    for(String removedDb : removedDatabases) {
                        RemovedDbBlacklistChunk chunk = new RemovedDbBlacklistChunk(removedDb,
                                                                                    getBlacklistedTables(removedDb, configTableBlacklist),
                            config, sourceInfo
                        );
                        configChunks.add(chunk);
                    }
                }
                // auto-resolve all new databases in the blacklist
                Collection<String> newBlacklistedDatabases = listRemove(configDbBlacklist, resolvedDbBlacklist);
                sourceInfo.addBlacklistedDatabases(newBlacklistedDatabases);
            }
        }
        return configChunks;
    }

    /**
     * Create and return {@link SnapshotableConfigChunk}s for tables. Will auto-resolve any tables that need no action to
     * be resolved.
     *
     * @param config the connector {@link Configuration }
     * @param sourceInfo the {@link SourceInfo}
     * @return a Collection of {@link SnapshotableConfigChunk}s for tables that need resolution.
     */
    private static Collection<SnapshotableConfigChunk> makeTableChunks(Configuration config,
                                                                       SourceInfo sourceInfo) {
        Collection<SnapshotableConfigChunk> configChunks = new ArrayList<>();

        Collection<String> configDbWhitelist = config.getStrings(MySqlConnectorConfig.DATABASE_WHITELIST, MySqlConnectorConfig.WHITELIST_BLACKLIST_DELIMITER);
        Collection<String> configDbBlacklist = config.getStrings(MySqlConnectorConfig.DATABASE_BLACKLIST, MySqlConnectorConfig.WHITELIST_BLACKLIST_DELIMITER);
        Collection<TableId> configTableWhitelist = config.getElements(MySqlConnectorConfig.TABLE_WHITELIST,
                                                                      MySqlConnectorConfig.WHITELIST_BLACKLIST_DELIMITER,
                                                                      TableId::parse);
        Collection<TableId> configTableBlacklist = config.getElements(MySqlConnectorConfig.TABLE_BLACKLIST,
                                                                      MySqlConnectorConfig.WHITELIST_BLACKLIST_DELIMITER,
                                                                      TableId::parse);

        Collection<String> resolvedDbWhitelist = sourceInfo.getResolvedDatabaseWhitelist();
        Collection<String> resolvedDbBlacklist = sourceInfo.getResolvedDatabaseBlacklist();
        Collection<TableId> resolvedTableWhitelist = sourceInfo.getResolvedTableWhitelist();
        Collection<TableId> resolvedTableBlacklist = sourceInfo.getResolvedTableBlacklist();

        Predicate<TableId> resolvedTableFilter = Selectors.tableSelector()
                                                          .includeDatabases(resolvedDbWhitelist)
                                                          .excludeDatabases(resolvedDbBlacklist)
                                                          .includeTables(resolvedTableWhitelist)
                                                          .excludeTables(resolvedTableBlacklist)
                                                          .build();
        Predicate<TableId> configTableFilter = Selectors.tableSelector()
                                                        .includeDatabases(configDbWhitelist)
                                                        .excludeDatabases(configDbBlacklist)
                                                        .includeTables(configTableWhitelist)
                                                        .excludeTables(configTableBlacklist)
                                                        .build();

        if (configTableWhitelist != null) {
            for(TableId whitelistedTable : configTableWhitelist) {
                if (configTableFilter.test(whitelistedTable) && !resolvedTableFilter.test(whitelistedTable)) {
                    // this table needs to be resolved but has not been
                    // (we need to verify the table needs to be resolved because it may be in a blacklisted database!)
                    SingleTableWhitelistChunk chunk = new SingleTableWhitelistChunk(whitelistedTable, config, sourceInfo);
                    configChunks.add(chunk);
                } else {
                    // if it is resolved, or nothing needs to happen to resolve it, make sure this table is marked as resolved.
                    sourceInfo.addWhitelistedTable(whitelistedTable);
                }
            }
        } else if (configTableBlacklist != null) {
            if (!resolvedTableWhitelist.isEmpty()) {
                // totally new blacklist; needs to be resolved in one complete sweep.
                AllTableBlacklistChunk chunk = new AllTableBlacklistChunk(configTableBlacklist, config, sourceInfo);
                configChunks.add(chunk);

            } else if (!resolvedTableBlacklist.isEmpty()){
                // maybe the blacklist has been modified
                Collection<TableId> removedTables = listRemove(resolvedTableBlacklist, configTableBlacklist);
                if (!removedTables.isEmpty()) {
                    // some tables have been removed from the blacklist. each removal can be resolved individually
                    for(TableId removedTable : removedTables) {
                        RemovedTableBlacklistChunk chunk = new RemovedTableBlacklistChunk(removedTable, config, sourceInfo);
                        configChunks.add(chunk);
                    }
                }
                // auto-resolve all new tables in the blacklist
                Collection<TableId> newBlacklistedTables = listRemove(configTableBlacklist, resolvedTableBlacklist);
                sourceInfo.addBlacklistedTables(newBlacklistedTables);
            }
        }
        return configChunks;
    }

    /**
     * @param a the base collection
     * @param b the elements to remove from a
     * @param <T> the element type
     * @return a new collection resulting from removing all elements of b from a
     */
    private static <T> Collection<T> listRemove(Collection<T> a, Collection<T> b) {
        Collection<T> aCopy = new ArrayList<>(a);
        aCopy.removeAll(b);
        return aCopy;
    }

    /**
     * Return the list of blacklisted tables in a given database.
     *
     * @param database the database
     * @param allBlacklistedTables the full list of blacklisted tables; may be null or empty
     * @return the list of blacklisted tables in the given whitelisted database; may be empty if none
     */
    private static Collection<TableId> getBlacklistedTables(String database,
                                                            Collection<TableId> allBlacklistedTables) {
        if (allBlacklistedTables == null || allBlacklistedTables.isEmpty()){
            return Collections.emptyList();
        } else {
            return allBlacklistedTables.stream()
                                       .filter(tableId -> database.equals(tableId.catalog()))
                                       .collect(Collectors.toCollection(ArrayList::new));
        }

    }
}
