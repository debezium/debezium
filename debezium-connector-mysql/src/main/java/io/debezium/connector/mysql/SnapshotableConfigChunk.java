package io.debezium.connector.mysql;

import io.debezium.relational.Selectors;
import io.debezium.relational.TableId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A class representing a single, independently snapshotable config chunk.
 * todo where should this live?
 */
public abstract class SnapshotableConfigChunk {

    protected SourceInfo sourceInfo;

    public SnapshotableConfigChunk(SourceInfo sourceInfo) {
        this.sourceInfo = sourceInfo;
    }

    /**
     * Creates and returns a reader to snapshot the given chunk.
     * @return a {@link Reader} for the given chunk of the config.
     */
    public abstract Reader createSnapshotReader();

    /**
     * Mark this chunk as resolved in the sourceInfo
     */
    public abstract void markChunkResolved();



    public static class SingleDbWhitelistChunk extends SnapshotableConfigChunk {

        private String whitelistedDb;
        private Collection<TableId> blacklistedTables;

        /**
         *
         * @param whitelistedDb the whitelisted DB that is the core of this chunk
         * @param blacklistedTables any blacklisted tables in the whitelisted DB
         * @param sourceInfo the {@link SourceInfo}
         */
        public SingleDbWhitelistChunk(String whitelistedDb,
                                      Collection<TableId> blacklistedTables,
                                      SourceInfo sourceInfo) {
            super(sourceInfo);
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

    public static class AllDbBlacklistChunk extends SnapshotableConfigChunk {

        private Collection<String> blacklistedDbs;
        private Collection<TableId> blacklistedTables;

        public AllDbBlacklistChunk(Collection<String> blacklistedDbs,
                                   Collection<TableId> blacklistedTables,
                                   SourceInfo sourceInfo) {
            super(sourceInfo);
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

    public static class SingleTableWhitelistChunk extends SnapshotableConfigChunk {

        private TableId whitelistedTableId;

        public SingleTableWhitelistChunk(TableId whitelistedTableId,
                                         SourceInfo sourceInfo) {
            super(sourceInfo);
            this.whitelistedTableId = whitelistedTableId;
        }

        @Override
        public Reader createSnapshotReader() {
            return null;
        }

        @Override
        public void markChunkResolved() {
            sourceInfo.addWhitelistedTable(whitelistedTableId);
        }
    }

    public static class AllTableBlacklistChunk extends SnapshotableConfigChunk {

        private Collection<TableId> blacklistedTables;

        public AllTableBlacklistChunk(Collection<TableId> blacklistedTables,
                                      SourceInfo sourceInfo) {
            super(sourceInfo);
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

    public static class RemovedDbBlacklistChunk extends SnapshotableConfigChunk {

        private String previouslyBlacklistedDb;
        private Collection<TableId> blacklistedTables;

        public RemovedDbBlacklistChunk(String previouslyBlacklistedDb,
                                       Collection<TableId> blacklistedTables,
                                       SourceInfo sourceInfo) {
            super(sourceInfo);
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

    public static class RemovedTableBlacklistChunk extends SnapshotableConfigChunk {

        private TableId previouslyBlacklistedTable;

        public RemovedTableBlacklistChunk(TableId previouslyBlacklistedTable,
                                       SourceInfo sourceInfo) {
            super(sourceInfo);
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
     * Divide the given config information into a collection of individually snapshotable chunks.  Auto-resolve
     * any config parts that need no action to be resolved.
     *
     * @param sourceInfo the {@link SourceInfo}
     * @param configDbWhitelist the configured database whitelist, if any; may be null
     * @param resolvedDbWhitelist the currently resolved database whitelist
     * @param configDbBlacklist the configured database blacklist, if any; may be null
     * @param resolvedDbBlacklist the currently resolved database blacklist
     * @param configTableWhitelist the configured table whitelist, if any; may be null
     * @param resolvedTableWhitelist the currently resolved table whitelist
     * @param configTableBlacklist the configured table blacklist, if any; may be null
     * @param resolvedTableBlacklist the currently resolved table blacklist
     * @return A Collection of {@link SnapshotableConfigChunk}s for everything that needs resolution.
     */
    public static Collection<SnapshotableConfigChunk> divideNewConfigIntoChunks(SourceInfo sourceInfo,
                                                                                Collection<String> configDbWhitelist,
                                                                                Collection<String> resolvedDbWhitelist,
                                                                                Collection<String> configDbBlacklist,
                                                                                Collection<String> resolvedDbBlacklist,
                                                                                Collection<TableId> configTableWhitelist,
                                                                                Collection<TableId> resolvedTableWhitelist,
                                                                                Collection<TableId> configTableBlacklist,
                                                                                Collection<TableId> resolvedTableBlacklist) {
        Predicate<String> resolvedDbFilter = Selectors.databaseSelector()
                                                      .includeDatabases(resolvedDbWhitelist)
                                                      .excludeDatabases(resolvedDbBlacklist)
                                                      .build();
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

        Collection<SnapshotableConfigChunk> configChunks = makeDatabaseChunks(sourceInfo,
                                                                              resolvedDbFilter,
                                                                              configDbWhitelist,
                                                                              resolvedDbWhitelist,
                                                                              configDbBlacklist,
                                                                              resolvedDbBlacklist,
                                                                              configTableBlacklist);

        configChunks.addAll(makeTableChunks(sourceInfo,
                                            resolvedTableFilter,
                                            configTableFilter,
                                            configTableWhitelist,
                                            resolvedTableWhitelist,
                                            configTableBlacklist,
                                            resolvedTableBlacklist));

        return configChunks;
    }

    /**
     * Create and return Snapshotable Config chunks for any databases that need resolving. Additionally, auto-resolve
     * any databases that do not need any action taken to be resolved (for example, a newly blacklisted database).
     *
     * @param sourceInfo the {@link SourceInfo}
     * @param resolvedDbFilter a predicate that will return true for databases that are resolved.
     * @param configDbWhitelist the configured db whitelist, if any; may be null.
     * @param resolvedDbWhitelist the currently resolved db whitelist.
     * @param configDbBlacklist the configured db whitelist, if any; may be null.
     * @param resolvedDbBlacklist the currently resolved db blacklist.
     * @param configTableBlacklist the currently configured table blacklist, if any; may be null.
     * @return a Collection of {@link SnapshotableConfigChunk}s for databases that need resolution.
     */
    private static Collection<SnapshotableConfigChunk> makeDatabaseChunks(SourceInfo sourceInfo,
                                                                          Predicate<String> resolvedDbFilter,
                                                                          Collection<String> configDbWhitelist,
                                                                          Collection<String> resolvedDbWhitelist,
                                                                          Collection<String> configDbBlacklist,
                                                                          Collection<String> resolvedDbBlacklist,
                                                                          Collection<TableId> configTableBlacklist) {
        Collection<SnapshotableConfigChunk> configChunks = new ArrayList<>();

        if (configDbWhitelist != null) {
            for (String whitelistedDb : configDbWhitelist) {
                if (!resolvedDbFilter.test(whitelistedDb)) {
                    // this database has not yet been resolved.
                    SingleDbWhitelistChunk chunk = new SingleDbWhitelistChunk(whitelistedDb,
                        getBlacklistedTables(whitelistedDb,
                            configTableBlacklist),
                        sourceInfo);
                    configChunks.add(chunk);
                } else {
                    // if it is resolved, make sure this db is marked as resolved.
                    sourceInfo.addWhitelistedDatabase(whitelistedDb);
                }
            }
        } else if (configDbBlacklist != null) {
            if (resolvedDbWhitelist != null) {
                // totally new blacklist; has to be resolved in one complete sweep.
                AllDbBlacklistChunk chunk = new AllDbBlacklistChunk(configDbBlacklist, configTableBlacklist, sourceInfo);
                configChunks.add(chunk);
            } else if (resolvedDbBlacklist != null) {
                // maybe the blacklist has been modified.
                Collection<String> removedDatabases = listRemove(resolvedDbBlacklist, configDbBlacklist);
                if (!removedDatabases.isEmpty()) {
                    // some databases have been removed from the blacklist. Each removal can be resolved individually.
                    for(String removedDb : removedDatabases) {
                        RemovedDbBlacklistChunk chunk = new RemovedDbBlacklistChunk(removedDb, getBlacklistedTables(removedDb, configTableBlacklist), sourceInfo);
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
     * @param sourceInfo the {@link SourceInfo}
     * @param resolvedTableFilter A {@link Predicate} that returns true for tables that are resolved.
     * @param configTableFilter a {@link Predicate} that returns true for tables that are in the current config.
     * @param configTableWhitelist the configured table whitelist, if any; may be null
     * @param resolvedTableWhitelist the currently resolved table whitelist.
     * @param configTableBlacklist the configured table blacklist, if any; may be null
     * @param resolvedTableBlacklist the currently resolved table blacklist.
     * @return a Collection of {@link SnapshotableConfigChunk}s for tables that need resolution.
     */
    private static Collection<SnapshotableConfigChunk> makeTableChunks(SourceInfo sourceInfo,
                                                                       Predicate<TableId> resolvedTableFilter,
                                                                       Predicate<TableId> configTableFilter,
                                                                       Collection<TableId> configTableWhitelist,
                                                                       Collection<TableId> resolvedTableWhitelist,
                                                                       Collection<TableId> configTableBlacklist,
                                                                       Collection<TableId> resolvedTableBlacklist) {
        Collection<SnapshotableConfigChunk> configChunks = new ArrayList<>();

        if (configTableWhitelist != null) {
            for(TableId whitelistedTable : configTableWhitelist) {
                if (configTableFilter.test(whitelistedTable) && !resolvedTableFilter.test(whitelistedTable)) {
                    // this table needs to be resolved but has not been
                    // (we need to verify the table needs to be resolved because it may be in a blacklisted database!)
                    SingleTableWhitelistChunk chunk = new SingleTableWhitelistChunk(whitelistedTable, sourceInfo);
                    configChunks.add(chunk);
                } else {
                    // if it is resolved, or nothing needs to happen to resolve it, make sure this table is marked as resolved.
                    sourceInfo.addWhitelistedTable(whitelistedTable);
                }
            }
        } else if (configTableBlacklist != null) {
            if (resolvedTableWhitelist != null) {
                // totally new blacklist; needs to be resolved in one complete sweep.
                AllTableBlacklistChunk chunk = new AllTableBlacklistChunk(configTableBlacklist, sourceInfo);
                configChunks.add(chunk);

            } else if (resolvedTableBlacklist != null){
                // maybe the blacklist has been modified
                Collection<TableId> removedTables = listRemove(resolvedTableBlacklist, configTableBlacklist);
                if (!removedTables.isEmpty()) {
                    // some tables have been removed from the blacklist. each removal can be resolved individually
                    for(TableId removedTable : removedTables) {
                        RemovedTableBlacklistChunk chunk = new RemovedTableBlacklistChunk(removedTable, sourceInfo);
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
