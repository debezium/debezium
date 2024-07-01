/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.ehcache;

import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_CACHE_PATH;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_CACHE_RECENTTRANSACTIONS_SIZE_MB;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_CACHE_SCHEMACHANGES_SIZE_MB;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_CACHE_TRANSACTION_SIZE_GB;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.CacheManagerConfiguration;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.statistics.CacheStatistics;
import org.ehcache.core.statistics.DefaultStatisticsService;
import org.ehcache.core.statistics.TierStatistics;
import org.ehcache.expiry.ExpiryPolicy;
import org.infinispan.commons.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.processor.AbstractLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.LogMinerEventProcessor;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;
import io.debezium.util.Loggings;

/**
 * A {@link LogMinerEventProcessor} that uses Ehcache to store events as they're being
 * processed and emitted from Oracle LogMiner.
 *
 */
public class EhcacheLogMinerEventProcessor extends AbstractLogMinerEventProcessor<EhcacheTransaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheLogMinerEventProcessor.class);
    private final EventDispatcher<OraclePartition, TableId> dispatcher;
    private final OraclePartition partition;
    private final OracleOffsetContext offsetContext;
    private final LogMinerStreamingChangeEventSourceMetrics metrics;
    private final StatisticsService transactionCacheStats;
    private final PersistentCacheManager cacheManager;

    /**
     * Cache of transactions, keyed based on the transaction's unique identifier
     */
    private final Cache<String, EhcacheTransaction> transactionCache;

    /**
     * Cache of processed transactions (committed or rolled back), keyed based on the transaction's unique identifier.
     */
    private final Cache<String, Scn> recentlyProcessedTransactionsCache;
    private final Cache<Scn, String> schemaChangesCache;

    public EhcacheLogMinerEventProcessor(ChangeEventSourceContext context,
                                         OracleConnectorConfig connectorConfig,
                                         OracleConnection jdbcConnection,
                                         EventDispatcher<OraclePartition, TableId> dispatcher,
                                         OraclePartition partition,
                                         OracleOffsetContext offsetContext,
                                         OracleDatabaseSchema schema,
                                         LogMinerStreamingChangeEventSourceMetrics metrics) {
        super(context, connectorConfig, schema, partition, offsetContext, dispatcher, metrics, jdbcConnection);
        this.dispatcher = dispatcher;
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.metrics = (LogMinerStreamingChangeEventSourceMetrics) metrics;

        this.transactionCacheStats = new DefaultStatisticsService();

        final int transactionCacheSizeGb = connectorConfig.getConfig().getInteger(LOG_MINING_BUFFER_EHCACHE_CACHE_TRANSACTION_SIZE_GB);
        final int recentTransactionsCacheSizeMb = connectorConfig.getConfig().getInteger(LOG_MINING_BUFFER_EHCACHE_CACHE_RECENTTRANSACTIONS_SIZE_MB);
        final int schemaChangesCacheSizeMb = connectorConfig.getConfig().getInteger(LOG_MINING_BUFFER_EHCACHE_CACHE_SCHEMACHANGES_SIZE_MB);

        final CacheConfiguration<String, EhcacheTransaction> transactionCacheConfig = CacheConfigurationBuilder
                .newCacheConfigurationBuilder(String.class, EhcacheTransaction.class,
                        ResourcePoolsBuilder.newResourcePoolsBuilder()
                                .disk(transactionCacheSizeGb, MemoryUnit.GB, false))
                .withExpiry(ExpiryPolicy.NO_EXPIRY)
                .build();

        final CacheConfiguration<String, Scn> recentlyProcessedTransactionsCacheConfig = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, Scn.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                        .disk(recentTransactionsCacheSizeMb, MemoryUnit.MB, false))
                .withExpiry(ExpiryPolicy.NO_EXPIRY)
                .build();

        final CacheConfiguration<Scn, String> schemaChangesCacheConfig = CacheConfigurationBuilder.newCacheConfigurationBuilder(Scn.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                        .disk(schemaChangesCacheSizeMb, MemoryUnit.MB, false))
                .withExpiry(ExpiryPolicy.NO_EXPIRY)
                .build();

        final CacheManagerConfiguration cacheManagerConf = CacheManagerBuilder.persistence(connectorConfig.getConfig().getString(LOG_MINING_BUFFER_EHCACHE_CACHE_PATH));

        this.cacheManager = (PersistentCacheManager) CacheManagerBuilder.newCacheManagerBuilder()
                .with(cacheManagerConf)
                .withCache("transactionCache", transactionCacheConfig)
                .withCache("recentlyProcessedTransactionsCache", recentlyProcessedTransactionsCacheConfig)
                .withCache("schemaChangesCache", schemaChangesCacheConfig)
                .using(transactionCacheStats)
                .build(true);

        this.recentlyProcessedTransactionsCache = cacheManager.getCache("recentlyProcessedTransactionsCache", String.class, Scn.class);
        this.schemaChangesCache = cacheManager.getCache("schemaChangesCache", Scn.class, String.class);
        this.transactionCache = cacheManager.getCache("transactionCache", String.class, EhcacheTransaction.class);
    }

    @Override
    protected Collection<EhcacheTransaction> transactionCacheValues() {
        return StreamSupport.stream(transactionCache.spliterator(), false)
                .map(t -> t.getValue())
                .collect(Collectors.toList());
    }

    @Override
    protected EhcacheTransaction transactionCacheGet(String key) {
        return (key == null) ? null : transactionCache.get(key);
    }

    @Override
    protected void transactionCachePut(String key, EhcacheTransaction value) {
        if (key != null && value != null) {
            transactionCache.put(key, value);
        }
    }

    private void transactionCacheReplace(String key, EhcacheTransaction value) {
        if (key != null && value != null) {
            transactionCache.replace(key, value);
        }
    }

    @Override
    protected int transactionCacheSize() {
        CacheStatistics statistics = transactionCacheStats.getCacheStatistics("transactionCache");
        return Long.valueOf(
                statistics.getTierStatistics().values().stream().mapToLong(TierStatistics::getMappings).sum())
                .intValue();
    }

    @Override
    protected boolean transactionCacheContainsKey(String key) {
        if (key == null) {
            return false;
        }
        return transactionCache.containsKey(key);
    }

    @Override
    protected Set<String> transactionCacheKeys() {
        return StreamSupport.stream(transactionCache.spliterator(), false)
                .map(t -> t.getKey())
                .collect(Collectors.toSet());
    }

    @Override
    protected Iterator transactionCacheIterator() {
        return transactionCache.iterator();
    }

    @Override
    protected EhcacheTransaction createTransaction(LogMinerEventRow row) {
        return new EhcacheTransaction(row.getTransactionId(), row.getScn(), row.getChangeTime(), row.getUserName(), row.getThread());
    }

    @Override
    protected void removeEventWithRowId(LogMinerEventRow row) {
        EhcacheTransaction transaction = transactionCacheGet(row.getTransactionId());
        if (transaction == null) {
            if (isTransactionIdWithNoSequence(row.getTransactionId())) {
                // This means that Oracle LogMiner found an event that should be undone but its corresponding
                // undo entry was read in a prior mining session and the transaction's sequence could not be
                // resolved. In this case, lets locate the transaction based solely on XIDUSN and XIDSLT.
                final String transactionPrefix = getTransactionIdPrefix(row.getTransactionId());
                LOGGER.debug("Undo change refers to a transaction that has no explicit sequence, '{}'", row.getTransactionId());
                LOGGER.debug("Checking all transactions with prefix '{}'", transactionPrefix);
                Iterator it = transactionCacheIterator();
                while (it.hasNext()) {
                    final String transactionKey = ((Cache.Entry<String, EhcacheTransaction>) it.next()).getKey();
                    if (transactionKey.startsWith(transactionPrefix)) {
                        transaction = transactionCacheGet(transactionKey);
                        if (transaction != null && transaction.removeEventWithRowId(row.getRowId())) {
                            // We successfully found a transaction with the same XISUSN and XIDSLT and that
                            // transaction included a change for the specified row id.
                            transactionCacheReplace(transactionKey, transaction);
                            Loggings.logDebugAndTraceRecord(LOGGER, row, "Undo change on table '{}' was applied to transaction '{}'", row.getTableId(), transactionKey);
                            return;
                        }
                    }
                }
                Loggings.logWarningAndTraceRecord(LOGGER, row, "Cannot undo change on table '{}' since event with row-id {} was not found", row.getTableId(),
                        row.getRowId());
            }
            else if (!getConfig().isLobEnabled()) {
                Loggings.logWarningAndTraceRecord(LOGGER, row, "Cannot undo change on table '{}' since transaction '{}' was not found.", row.getTableId(),
                        row.getTransactionId());
            }
        }
        else {
            if (!transaction.removeEventWithRowId(row.getRowId())) {
                Loggings.logWarningAndTraceRecord(LOGGER, row, "Cannot undo change on table '{}' since event with row-id {} was not found.", row.getTableId(),
                        row.getRowId());
            }
        }
    }

    @Override
    public void close() throws Exception {
        this.cacheManager.close();
    }

    @Override
    protected boolean isRecentlyProcessed(String transactionId) {
        return (transactionId == null) ? false : recentlyProcessedTransactionsCache.containsKey(transactionId);
    }

    @Override
    protected boolean hasSchemaChangeBeenSeen(LogMinerEventRow row) {
        return (row == null) ? false : schemaChangesCache.containsKey(row.getScn());
    }

    @Override
    protected EhcacheTransaction getAndRemoveTransactionFromCache(String transactionId) {
        if (transactionId == null) {
            return null;
        }
        var transaction = transactionCache.get(transactionId);
        transactionCache.remove(transactionId);
        return transaction;
    }

    @Override
    protected Iterator<LogMinerEvent> getTransactionEventIterator(EhcacheTransaction transaction) {
        return transaction.getEvents().iterator();
    }

    @Override
    protected void finalizeTransactionCommit(String transactionId, Scn commitScn) {
        getAbandonedTransactionsCache().remove(transactionId);
        if (getConfig().isLobEnabled()) {
            // cache recently committed transactions by transaction id
            recentlyProcessedTransactionsCache.put(transactionId, commitScn);
        }
    }

    @Override
    protected void finalizeTransactionRollback(String transactionId, Scn rollbackScn) {
        transactionCache.remove(transactionId);
        getAbandonedTransactionsCache().remove(transactionId);
        if (getConfig().isLobEnabled()) {
            recentlyProcessedTransactionsCache.put(transactionId, rollbackScn);
        }
    }

    @Override
    protected String getFirstActiveTransactionKey() {
        Iterator<Cache.Entry<String, EhcacheTransaction>> it = transactionCache.iterator();
        return it.hasNext() ? it.next().getKey() : null;
    }

    @Override
    protected void handleSchemaChange(LogMinerEventRow row) throws InterruptedException {
        super.handleSchemaChange(row);
        if (row.getTableName() != null && getConfig().isLobEnabled()) {
            schemaChangesCache.put(row.getScn(), null);
        }
    }

    @Override
    protected void addToTransaction(String transactionId, LogMinerEventRow row, Supplier<LogMinerEvent> eventSupplier) {
        if (getAbandonedTransactionsCache().contains(transactionId)) {
            LOGGER.warn("Event for abandoned transaction {}, skipped.", transactionId);
            return;
        }
        if (!isRecentlyProcessed(transactionId)) {
            EhcacheTransaction transaction = transactionCacheGet(transactionId);
            if (transaction == null) {
                LOGGER.trace("Transaction {} not in cache for DML, creating.", transactionId);
                transaction = createTransaction(row);
                transactionCachePut(transactionId, transaction);
            }

            if (isTransactionOverEventThreshold(transaction)) {
                abandonTransactionOverEventThreshold(transaction);
                return;
            }

            int eventId = transaction.getNextEventId();
            if (transaction.getEvents().size() <= eventId) {
                // Add new event at eventId offset
                LOGGER.trace("Transaction {}, adding event reference at index {}", transactionId, eventId);
                transaction.getEvents().add(eventSupplier.get());
                transactionCacheReplace(transactionId, transaction);
                metrics.calculateLagFromSource(row.getChangeTime());
            }

            CacheStatistics statistics = transactionCacheStats.getCacheStatistics("transactionCache");
            long transactionsCount = statistics.getTierStatistics().values().stream().mapToLong(TierStatistics::getMappings).sum();
            metrics.setActiveTransactionCount(transactionsCount);
        }
        else if (!getConfig().isLobEnabled()) {
            // Explicitly only log this warning when LobEnabled is false because its commonplace for a
            // transaction to be re-mined and therefore seen as already processed until the SCN low
            // watermark is advanced after a long transaction is committed.
            LOGGER.warn("Event for transaction {} has already been processed, skipped.", transactionId);
        }
    }

    @Override
    protected int getTransactionEventCount(EhcacheTransaction transaction) {
        return transaction.getEvents().size();
    }

    @Override
    protected PreparedStatement createQueryStatement() throws SQLException {
        return jdbcConnection.connection().prepareStatement(getQueryString(),
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    protected Scn calculateNewStartScn(Scn endScn, Scn maxCommittedScn) throws InterruptedException {
        if (getConfig().isLobEnabled()) {
            if (!transactionCache.iterator().hasNext() && !maxCommittedScn.isNull()) {
                offsetContext.setScn(maxCommittedScn);
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }
            else {
                abandonTransactions(getConfig().getLogMiningTransactionRetention());
                final Scn minStartScn = getTransactionCacheMinimumScn();
                if (!minStartScn.isNull()) {
                    StreamSupport.stream(recentlyProcessedTransactionsCache.spliterator(), false)
                            .filter(e -> e.getValue().compareTo(minStartScn) < 0)
                            .forEach(f -> recentlyProcessedTransactionsCache.remove(f.getKey()));

                    StreamSupport.stream(schemaChangesCache.spliterator(), false)
                            .filter(e -> e.getKey().compareTo(minStartScn) < 0)
                            .forEach(f -> schemaChangesCache.remove(f.getKey()));

                    offsetContext.setScn(minStartScn.subtract(Scn.valueOf(1)));
                    dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
                }
            }
            return offsetContext.getScn();
        }
        else {
            if (!getLastProcessedScn().isNull() && getLastProcessedScn().compareTo(endScn) < 0) {
                // If the last processed SCN is before the endScn we need to use the last processed SCN as the
                // next starting point as the LGWR buffer didn't flush all entries from memory to disk yet.
                endScn = getLastProcessedScn();
            }

            if (!transactionCache.iterator().hasNext()) {
                offsetContext.setScn(endScn);
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }
            else {
                abandonTransactions(getConfig().getLogMiningTransactionRetention());
                final Scn minStartScn = getTransactionCacheMinimumScn();
                if (!minStartScn.isNull()) {
                    offsetContext.setScn(minStartScn.subtract(Scn.valueOf(1)));
                    dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
                }
            }
            return endScn;
        }
    }

    @Override
    protected Scn getTransactionCacheMinimumScn() {
        return StreamSupport.stream(transactionCache.spliterator(), false)
                .map(c -> c.getValue().getStartScn())
                .min(Scn::compareTo)
                .orElse(Scn.NULL);
    }

    @Override
    protected Optional<EhcacheTransaction> getOldestTransactionInCache() {
        EhcacheTransaction transaction = null;
        if (transactionCache.iterator().hasNext()) {
            // Seed with the first element
            transaction = transactionCache.iterator().next().getValue();
            var it = transactionCache.iterator();
            while (it.hasNext()) {
                EhcacheTransaction entry = it.next().getValue();
                int comparison = entry.getStartScn().compareTo(transaction.getStartScn());
                if (comparison < 0) {
                    // if entry has a smaller scn, it came before.
                    transaction = entry;
                }
                else if (comparison == 0) {
                    // if entry has an equal scn, compare the change times.
                    if (entry.getChangeTime().isBefore(transaction.getChangeTime())) {
                        transaction = entry;
                    }
                }
            }
        }
        return Optional.ofNullable(transaction);
    }

    @Override
    public void abandonTransactions(Duration retention) throws InterruptedException {
        if (!Duration.ZERO.equals(retention)) {
            Optional<Scn> lastScnToAbandonTransactions = getLastScnToAbandon(jdbcConnection, retention);
            if (lastScnToAbandonTransactions.isPresent()) {
                Scn thresholdScn = lastScnToAbandonTransactions.get();
                Scn smallestScn = getTransactionCacheMinimumScn();
                if (!smallestScn.isNull() && thresholdScn.compareTo(smallestScn) >= 0) {
                    boolean first = true;
                    Iterator<Cache.Entry<String, EhcacheTransaction>> iterator = transactionCache.iterator();
                    try {
                        while (iterator.hasNext()) {
                            Cache.Entry<String, EhcacheTransaction> entry = iterator.next();
                            if (entry.getValue().getStartScn().compareTo(thresholdScn) <= 0) {
                                if (first) {
                                    LOGGER.warn("All transactions with SCN <= {} will be abandoned.", thresholdScn);
                                    if (LOGGER.isDebugEnabled()) {
                                        try (Stream<String> s = transactionCacheKeys().stream()) {
                                            LOGGER.debug("List of transactions in the cache before transactions being abandoned: [{}]",
                                                    s.collect(Collectors.joining(",")));
                                        }
                                    }
                                    first = false;
                                }
                                LOGGER.warn("Transaction {} (start SCN {}, change time {}, redo thread {}, {} events) is being abandoned.",
                                        entry.getKey(), entry.getValue().getStartScn(), entry.getValue().getChangeTime(),
                                        entry.getValue().getRedoThreadId(), entry.getValue().getNumberOfEvents());

                                cleanupAfterTransactionRemovedFromCache(entry.getValue(), true);
                                iterator.remove();

                                metrics.addAbandonedTransactionId(entry.getKey());
                                metrics.setActiveTransactionCount(transactionCacheSize());
                            }
                        }
                    }
                    finally {
                        if (iterator instanceof CloseableIterator) {
                            ((CloseableIterator<Cache.Entry<String, EhcacheTransaction>>) iterator).close();
                        }
                    }
                    if (LOGGER.isDebugEnabled()) {
                        try (Stream<String> s = transactionCacheKeys().stream()) {
                            LOGGER.debug("List of transactions in the cache after transactions being abandoned: [{}]",
                                    s.collect(Collectors.joining(",")));
                        }
                    }

                    // Update the oldest scn metric are transaction abandonment
                    final Optional<EhcacheTransaction> oldestTransaction = getOldestTransactionInCache();
                    if (oldestTransaction.isPresent()) {
                        final EhcacheTransaction transaction = oldestTransaction.get();
                        metrics.setOldestScnDetails(transaction.getStartScn(), transaction.getChangeTime());
                    }
                    else {
                        metrics.setOldestScnDetails(Scn.NULL, null);
                    }

                    offsetContext.setScn(thresholdScn);
                }
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }
        }
    }
}
