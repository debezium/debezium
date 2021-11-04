/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.infinispan;

import java.util.Iterator;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.processor.TransactionCache;

/**
 * A {@link TransactionCache} implementation for use with embedded Infinispan.
 *
 * @author Chris Cranford
 */
public class InfinispanTransactionCache implements TransactionCache<InfinispanTransaction, Cache.Entry<String, InfinispanTransaction>> {

    private final Cache<String, InfinispanTransaction> cache;

    public InfinispanTransactionCache(Cache<String, InfinispanTransaction> cache) {
        this.cache = cache;
    }

    @Override
    public InfinispanTransaction get(String transactionId) {
        return cache.get(transactionId);
    }

    @Override
    public void put(String transactionId, InfinispanTransaction transaction) {
        cache.put(transactionId, transaction);
    }

    @Override
    public InfinispanTransaction remove(String transactionId) {
        return cache.remove(transactionId);
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public boolean isEmpty() {
        return cache.isEmpty();
    }

    @Override
    public Iterator<Cache.Entry<String, InfinispanTransaction>> iterator() {
        return cache.entrySet().iterator();
    }

    @Override
    public Scn getMinimumScn() {
        Scn minimumScn = Scn.NULL;
        try (CloseableIterator<InfinispanTransaction> iterator = cache.values().iterator()) {
            while (iterator.hasNext()) {
                final Scn transactionScn = iterator.next().getStartScn();
                if (minimumScn.isNull()) {
                    minimumScn = transactionScn;
                }
                else {
                    if (transactionScn.compareTo(minimumScn) < 0) {
                        minimumScn = transactionScn;
                    }
                }
            }
        }
        return minimumScn;
    }

    @Override
    public void close() throws Exception {
        cache.stop();
    }
}
