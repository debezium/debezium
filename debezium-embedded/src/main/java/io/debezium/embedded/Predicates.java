/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;

/**
 * Composite class representing predicate definitions.
 *
 * @author Jeremy Ford
 */
public class Predicates implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Predicates.class);

    private static final String TYPE_SUFFIX = ".type";

    private final Map<String, Predicate<SourceRecord>> predicates = new HashMap<>();

    public Predicates(Configuration config) {
        final String predicateList = config.getString(EmbeddedEngineConfig.PREDICATES);
        if (predicateList == null) {
            return;
        }
        for (String predicateName : predicateList.split(",")) {
            predicateName = predicateName.trim();
            final Predicate<SourceRecord> predicate = createPredicate(config, predicateName);
            predicates.put(predicateName, predicate);
        }
    }

    public Predicate<SourceRecord> getPredicate(String name) {
        return this.predicates.get(name);
    }

    @SuppressWarnings("unchecked")
    private static Predicate<SourceRecord> createPredicate(Configuration config, String name) {
        Predicate<SourceRecord> predicate;

        String predicatePrefix = predicateConfigNamespace(name);

        try {
            predicate = config.getInstance(predicatePrefix + TYPE_SUFFIX, Predicate.class);
        }
        catch (Exception e) {
            throw new DebeziumException("Error while instantiating predicate '" + name + "'", e);
        }

        if (predicate == null) {
            throw new DebeziumException("Cannot instantiate predicate '" + name + "'");
        }

        predicate.configure(config.subset(predicatePrefix, true).asMap());

        return predicate;
    }

    private static String predicateConfigNamespace(final String name) {
        return EmbeddedEngineConfig.PREDICATES.name() + "." + name;
    }

    @Override
    public void close() throws IOException {
        for (Predicate<SourceRecord> p : predicates.values()) {
            try {
                p.close();
            }
            catch (Exception e) {
                LOGGER.warn("Error while closing predicate", e);
            }
        }
    }
}
