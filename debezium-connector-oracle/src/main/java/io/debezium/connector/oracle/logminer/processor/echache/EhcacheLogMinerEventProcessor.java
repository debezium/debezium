/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.echache;

import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_CACHE_PATH;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_CACHE_RECENTTRANSACTIONS_SIZE_MB;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_CACHE_SCHEMACHANGES_SIZE_MB;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_CACHE_TRANSACTION_SIZE_MB;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;

import org.ehcache.PersistentCacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.CacheManagerConfiguration;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.statistics.DefaultStatisticsService;
import org.ehcache.core.util.ByteBufferInputStream;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LobEraseEvent;
import io.debezium.connector.oracle.logminer.events.LobWriteEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.events.RedoSqlDmlEvent;
import io.debezium.connector.oracle.logminer.events.SelectLobLocatorEvent;
import io.debezium.connector.oracle.logminer.events.TruncateEvent;
import io.debezium.connector.oracle.logminer.events.XmlBeginEvent;
import io.debezium.connector.oracle.logminer.events.XmlEndEvent;
import io.debezium.connector.oracle.logminer.events.XmlWriteEvent;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;
import io.debezium.connector.oracle.logminer.processor.AbstractTransactionCachingLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.LogMinerCache;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;

public class EhcacheLogMinerEventProcessor extends AbstractTransactionCachingLogMinerEventProcessor<EhcacheTransaction> {

    private final StatisticsService transactionCacheStats = new DefaultStatisticsService();
    private final PersistentCacheManager cacheManager;

    /**
     * Cache of transactions, keyed based on the transaction's unique identifier
     */
    private final LogMinerCache<String, EhcacheTransaction> transactionCache;

    /**
     * Cache of processed transactions (committed or rolled back), keyed based on the transaction's unique identifier.
     */
    private final LogMinerCache<String, String> recentlyProcessedTransactionsCache;
    private final LogMinerCache<String, String> schemaChangesCache;
    private final LogMinerCache<String, LogMinerEvent> eventCache;

    public EhcacheLogMinerEventProcessor(ChangeEventSource.ChangeEventSourceContext context,
                                         OracleConnectorConfig connectorConfig,
                                         OracleConnection jdbcConnection,
                                         EventDispatcher<OraclePartition, TableId> dispatcher,
                                         OraclePartition partition,
                                         OracleOffsetContext offsetContext,
                                         OracleDatabaseSchema schema,
                                         LogMinerStreamingChangeEventSourceMetrics metrics) {
        super(context, connectorConfig, jdbcConnection, dispatcher, partition, offsetContext, schema, metrics);

        final int transactionCacheSizeMb = connectorConfig.getConfig().getInteger(LOG_MINING_BUFFER_EHCACHE_CACHE_TRANSACTION_SIZE_MB);
        final int recentTransactionsCacheSizeMb = connectorConfig.getConfig().getInteger(LOG_MINING_BUFFER_EHCACHE_CACHE_RECENTTRANSACTIONS_SIZE_MB);
        final int schemaChangesCacheSizeMb = connectorConfig.getConfig().getInteger(LOG_MINING_BUFFER_EHCACHE_CACHE_SCHEMACHANGES_SIZE_MB);

        final ObjectMapper objectMapper = new ObjectMapper()
                .findAndRegisterModules();

        final CacheConfiguration<String, EhcacheTransaction> transactionCacheConfig = CacheConfigurationBuilder
                .newCacheConfigurationBuilder(String.class, EhcacheTransaction.class,
                        ResourcePoolsBuilder.newResourcePoolsBuilder()
                                .disk(transactionCacheSizeMb, MemoryUnit.MB, false))
                .withExpiry(ExpiryPolicy.NO_EXPIRY)
                .withValueSerializer(new Serializer<>() {
                    @Override
                    public ByteBuffer serialize(EhcacheTransaction ehcacheTransaction) throws SerializerException {
                        try {
                            return ByteBuffer.wrap(
                                    objectMapper.writeValueAsBytes(ehcacheTransaction));
                        }
                        catch (JsonProcessingException e) {
                            throw new SerializerException(e);
                        }
                    }

                    @Override
                    public EhcacheTransaction read(ByteBuffer byteBuffer) throws ClassNotFoundException, SerializerException {

                        try {
                            return objectMapper.readValue(new ByteBufferInputStream(byteBuffer), EhcacheTransaction.class);
                        }
                        catch (IOException e) {
                            throw new SerializerException(e);
                        }
                    }

                    @Override
                    public boolean equals(EhcacheTransaction ehcacheTransaction, ByteBuffer byteBuffer) throws ClassNotFoundException, SerializerException {
                        return ehcacheTransaction.equals(read(byteBuffer));
                    }
                })
                .build();

        final CacheConfiguration<String, String> recentlyProcessedTransactionsCacheConfig = CacheConfigurationBuilder
                .newCacheConfigurationBuilder(String.class, String.class,
                        ResourcePoolsBuilder.newResourcePoolsBuilder()
                                .disk(recentTransactionsCacheSizeMb, MemoryUnit.MB, false))
                .withExpiry(ExpiryPolicy.NO_EXPIRY)
                .build();

        final CacheConfiguration<String, String> schemaChangesCacheConfig = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                        .disk(schemaChangesCacheSizeMb, MemoryUnit.MB, false))
                .withExpiry(ExpiryPolicy.NO_EXPIRY)
                .build();

        final CacheConfiguration<String, LogMinerEvent> eventCacheConfig = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, LogMinerEvent.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                        // .heap(100))
                        .disk(schemaChangesCacheSizeMb, MemoryUnit.MB, true))
                .withExpiry(ExpiryPolicy.NO_EXPIRY)
                .withValueSerializer(new Serializer<>() {
                    @Override
                    public ByteBuffer serialize(LogMinerEvent event) throws SerializerException {
                        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                        DataOutputStream dataOutputStream = new DataOutputStream(
                                byteArrayOutputStream);
                        // dataOutputStream.writeBytes();

                        ByteBuffer buffer = ByteBuffer.allocate(1000);
                        try {
                            LogMinerObjectType logMinerObjectType = toLogMinerObjectType(event);
                            dataOutputStream.writeInt(logMinerObjectType.ordinal());
                            dataOutputStream.writeInt(event.getEventType().getValue());
                            dataOutputStream.writeUTF(event.getTableId().table());
                            dataOutputStream.writeUTF(event.getTableId().schema());
                            dataOutputStream.writeUTF(event.getTableId().catalog());
                            dataOutputStream.writeUTF(event.getRowId());
                            dataOutputStream.writeUTF(event.getRsId());
                            dataOutputStream.writeLong(event.getScn().isNull() ? 0L : event.getScn().longValue());
                            dataOutputStream.writeUTF(event.getChangeTime().toString());

                            if (event instanceof DmlEvent) {
                                DmlEvent dmlEvent = (DmlEvent) event;
                                LogMinerDmlEntry dmlEntry = dmlEvent.getDmlEntry();
                                // dataOutputStream.writeInt(dmlEntry.getEventType().getValue());
                                dataOutputStream.writeUTF(stringArrayToString(objectMapper, objectArrayToStringArray(dmlEntry.getNewValues())));
                                dataOutputStream.writeUTF(stringArrayToString(objectMapper, objectArrayToStringArray(dmlEntry.getOldValues())));
                                dataOutputStream.writeUTF(dmlEntry.getObjectName());
                                dataOutputStream.writeUTF(dmlEntry.getObjectOwner());

                                // LogMinerDmlEntryImpl(int eventType, Object[] newValues, Object[] oldValues, String owner, String name) {

                                // message LogMinerDmlEntryImpl {
                                // * required int32 operation = 1;
                                // * string newValues = 2;
                                // * string oldValues = 3;
                                // * required string name = 4;
                                // * required string owner = 5;
                                // * }
                            }
                            // dataOutputStream.close();
                            byteArrayOutputStream.close();

                            buffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
                            return buffer;
                        }
                        catch (IOException e) {
                            throw new SerializerException(e);
                        }

                        // try {
                        // return ByteBuffer.wrap(
                        // objectMapper.writeValueAsBytes(event)
                        // );
                        // } catch (JsonProcessingException e) {
                        // throw new SerializerException(e);
                        // }
                    }

                    @Override
                    public LogMinerEvent read(ByteBuffer byteBuffer) throws ClassNotFoundException, SerializerException {
                        DataInputStream dataInputStream = new DataInputStream(
                                new ByteBufferInputStream(byteBuffer));
                        try {
                            LogMinerObjectType logMinerObjectType = LogMinerObjectType.values()[dataInputStream.readInt()];
                            EventType eventType = EventType.from(dataInputStream.readInt());

                            String table = dataInputStream.readUTF();
                            String schema = dataInputStream.readUTF();
                            String catalog = dataInputStream.readUTF();
                            TableId tableId = new TableId(
                                    catalog, schema, table);

                            String rowId = dataInputStream.readUTF();
                            String rsId = dataInputStream.readUTF();
                            long scn = dataInputStream.readLong();
                            Instant changeTime = Instant.parse(dataInputStream.readUTF());

                            Object[] newValues = stringArrayToObjectArray(stringToStringArray(objectMapper, dataInputStream.readUTF()));
                            Object[] oldValues = stringArrayToObjectArray(stringToStringArray(objectMapper, dataInputStream.readUTF()));
                            String owner = dataInputStream.readUTF();
                            String name = dataInputStream.readUTF();

                            switch (logMinerObjectType) {
                                case DML:
                                    return new DmlEvent(
                                            eventType, Scn.valueOf(scn), tableId, rowId, rsId, changeTime,
                                            new LogMinerDmlEntryImpl(eventType.getValue(), newValues, oldValues, owner, name));
                                default:
                                    throw new UnsupportedOperationException();
                            }

                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        // LogMinerObjectType logMinerObjectType = LogMinerObjectType.values()[byteBuffer.getInt()];
                        // EventType eventType = EventType.values()[byteBuffer.getInt()];
                        // String table = byteBuffer.get

                        // try {
                        // return objectMapper.readValue(new ByteBufferInputStream(byteBuffer), LogMinerEvent.class);
                        // } catch (IOException e) {
                        // throw new SerializerException(e);
                        // }
                    }

                    @Override
                    public boolean equals(LogMinerEvent event, ByteBuffer byteBuffer) throws ClassNotFoundException, SerializerException {
                        return event.equals(read(byteBuffer));
                    }
                })
                .build();

        final CacheManagerConfiguration cacheManagerConf = CacheManagerBuilder.persistence(connectorConfig.getConfig().getString(LOG_MINING_BUFFER_EHCACHE_CACHE_PATH));

        this.cacheManager = (PersistentCacheManager) CacheManagerBuilder.newCacheManagerBuilder()
                .with(cacheManagerConf)
                .withCache("transactionCache", transactionCacheConfig)
                .withCache("recentlyProcessedTransactionsCache", recentlyProcessedTransactionsCacheConfig)
                .withCache("schemaChangesCache", schemaChangesCacheConfig)
                .withCache("eventCache", eventCacheConfig)
                .using(transactionCacheStats)
                .build(true);

        this.recentlyProcessedTransactionsCache = new EhcacheLogMinerCache<>(
                cacheManager.getCache("recentlyProcessedTransactionsCache", String.class, String.class));
        this.schemaChangesCache = new EhcacheLogMinerCache<>(
                cacheManager.getCache("schemaChangesCache", String.class, String.class));
        this.transactionCache = new EhcacheLogMinerCache<>(
                cacheManager.getCache("transactionCache", String.class, EhcacheTransaction.class));
        this.eventCache = new EhcacheLogMinerCache<>(
                cacheManager.getCache("eventCache", String.class, LogMinerEvent.class));
    }

    private String[] stringToStringArray(ObjectMapper objectMapper, String s) {
        try {
            return objectMapper.readValue(s, String[].class);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private LogMinerObjectType toLogMinerObjectType(LogMinerEvent event) {
        Class<? extends LogMinerEvent> eventClass = event.getClass();
        if (eventClass == XmlWriteEvent.class) {
            return LogMinerObjectType.XML_WRITE;
        }
        else if (eventClass == LobWriteEvent.class) {
            return LogMinerObjectType.LOB_WRITE;
        }
        else if (eventClass == DmlEvent.class) {
            return LogMinerObjectType.DML;
        }
        else if (eventClass == RedoSqlDmlEvent.class) {
            return LogMinerObjectType.DML_REDO;
        }
        else if (eventClass == XmlBeginEvent.class) {
            return LogMinerObjectType.DML_XML_BEGIN;
        }
        else if (eventClass == TruncateEvent.class) {
            return LogMinerObjectType.DML_TRUNCATE;
        }
        else if (eventClass == SelectLobLocatorEvent.class) {
            return LogMinerObjectType.DML_LOB_LOCATOR_EVENT;
        }
        else if (eventClass == XmlEndEvent.class) {
            return LogMinerObjectType.XML_END;
        }
        else if (eventClass == LobEraseEvent.class) {
            return LogMinerObjectType.LOB_ERASE_EVENT;
        }
        throw new UnsupportedOperationException();
    }

    @Override
    protected EhcacheTransaction createTransaction(LogMinerEventRow row) {
        return new EhcacheTransaction(row.getTransactionId(), row.getScn(), row.getChangeTime(), row.getUserName(), row.getThread());
    }

    @Override
    public void close() throws Exception {
        this.cacheManager.close();
    }

    @Override
    public LogMinerCache<String, EhcacheTransaction> getTransactionCache() {
        return transactionCache;
    }

    @Override
    public LogMinerCache<String, LogMinerEvent> getEventCache() {
        return eventCache;
    }

    @Override
    public LogMinerCache<String, String> getSchemaChangesCache() {
        return schemaChangesCache;
    }

    @Override
    public LogMinerCache<String, String> getProcessedTransactionsCache() {
        return recentlyProcessedTransactionsCache;
    }

    // @Override
    // protected Scn calculateNewStartScn(Scn endScn, Scn maxCommittedScn) throws InterruptedException {
    // if (getConfig().isLobEnabled()) {
    // if (!transactionCache.iterator().hasNext() && !maxCommittedScn.isNull()) {
    // offsetContext.setScn(maxCommittedScn);
    // dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
    // }
    // else {
    // abandonTransactions(getConfig().getLogMiningTransactionRetention());
    // final Scn minStartScn = getTransactionCacheMinimumScn();
    // if (!minStartScn.isNull()) {
    // StreamSupport.stream(recentlyProcessedTransactionsCache.spliterator(), false)
    // .filter(e -> e.getValue().compareTo(minStartScn) < 0)
    // .forEach(f -> recentlyProcessedTransactionsCache.remove(f.getKey()));
    //
    // StreamSupport.stream(schemaChangesCache.spliterator(), false)
    // .filter(e -> e.getKey().compareTo(minStartScn) < 0)
    // .forEach(f -> schemaChangesCache.remove(f.getKey()));
    //
    // offsetContext.setScn(minStartScn.subtract(Scn.valueOf(1)));
    // dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
    // }
    // }
    // return offsetContext.getScn();
    // }
    // else {
    // if (!getLastProcessedScn().isNull() && getLastProcessedScn().compareTo(endScn) < 0) {
    // // If the last processed SCN is before the endScn we need to use the last processed SCN as the
    // // next starting point as the LGWR buffer didn't flush all entries from memory to disk yet.
    // endScn = getLastProcessedScn();
    // }
    //
    // if (!transactionCache.iterator().hasNext()) {
    // offsetContext.setScn(endScn);
    // dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
    // }
    // else {
    // abandonTransactions(getConfig().getLogMiningTransactionRetention());
    // final Scn minStartScn = getTransactionCacheMinimumScn();
    // if (!minStartScn.isNull()) {
    // offsetContext.setScn(minStartScn.subtract(Scn.valueOf(1)));
    // dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
    // }
    // }
    // return endScn;
    // }
    // }

    /**
     * Arrays cannot be serialized with null values and so we use a sentinel value
     * to mark a null element in an array.
     */
    private static final String NULL_VALUE_SENTINEL = "$$DBZ-NULL$$";

    /**
     * The supplied value arrays can now be populated with {@link OracleValueConverters#UNAVAILABLE_VALUE}
     * which is simple java object.  This cannot be represented as a string in the cached Infinispan record
     * and so this sentinel is used to translate the runtime object representation to a serializable form
     * and back during cache to object conversion.
     */
    private static final String UNAVAILABLE_VALUE_SENTINEL = "$$DBZ-UNAVAILABLE-VALUE$$";

    private static Object[] stringArrayToObjectArray(String[] values) {
        Object[] results = Arrays.copyOf(values, values.length, Object[].class);
        for (int i = 0; i < results.length; ++i) {
            if (results[i].equals(NULL_VALUE_SENTINEL)) {
                results[i] = null;
            }
            else if (results[i].equals(UNAVAILABLE_VALUE_SENTINEL)) {
                results[i] = OracleValueConverters.UNAVAILABLE_VALUE;
            }
        }
        return results;
    }

    /**
     * Converts the provided object-array to a string-array.
     *
     * Internally this method examines the supplied object array and handles conversion for {@literal null}
     * and {@link OracleValueConverters#UNAVAILABLE_VALUE} values so that they can be serialized.
     *
     * @param values the values array to be converted, should never be {@code null}
     * @return the values array converted to a string-array
     */
    private static String[] objectArrayToStringArray(Object[] values) {
        String[] results = new String[values.length];
        for (int i = 0; i < values.length; ++i) {
            if (values[i] == null) {
                results[i] = NULL_VALUE_SENTINEL;
            }
            else if (values[i] == OracleValueConverters.UNAVAILABLE_VALUE) {
                results[i] = UNAVAILABLE_VALUE_SENTINEL;
            }
            else {
                results[i] = (String) values[i];
            }
        }
        return results;
    }

    private static String stringArrayToString(ObjectMapper objectMapper, String[] values) {
        try {
            return objectMapper.writeValueAsString(values);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
