/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.config.Configuration;

/**
 * Composite class representing transformation chain.
 *
 * @author Jiri Pechanec
 */
public class Transformations implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Transformations.class);

    private static final String TYPE_SUFFIX = ".type";

    private static final String PREDICATE_SUFFIX = ".predicate";
    private static final String NEGATE_SUFFIX = ".negate";

    private final Configuration config;
    private final List<Transformation<SourceRecord>> transforms = new ArrayList<>();
    private final Predicates predicates;
    private final boolean tracingEnabled;
    private final Set<String> tracedAliases;

    public Transformations(Configuration config) {
        this.config = config;
        this.predicates = new Predicates(config);
        this.tracedAliases = new HashSet<>(config.getList(EmbeddedEngineConfig.TRANSFORMS_TRACING_INCLUDE));
        this.tracedAliases.removeIf(String::isEmpty);
        this.tracingEnabled = config.getBoolean(EmbeddedEngineConfig.TRANSFORMS_TRACING_ENABLED) && isOpenTelemetryAvailable();
        final String transformationList = config.getString(EmbeddedEngineConfig.TRANSFORMS);
        if (transformationList == null) {
            return;
        }
        for (String transfName : transformationList.split(",")) {
            transfName = transfName.trim();
            final Transformation<SourceRecord> transformation = getTransformation(transfName);
            transforms.add(transformation);
        }
    }

    private static String transformationConfigNamespace(final String name) {
        return EmbeddedEngineConfig.TRANSFORMS.name() + "." + name;
    }

    @VisibleForTesting
    @SuppressWarnings("unchecked")
    Transformation<SourceRecord> getTransformation(String name) {
        Transformation<SourceRecord> transformation;
        String transformPrefix = transformationConfigNamespace(name);

        try {
            transformation = config.getInstance(transformPrefix + TYPE_SUFFIX, Transformation.class);
        }
        catch (Exception e) {
            throw new DebeziumException("Error while instantiating transformation '" + name + "'", e);
        }

        if (transformation == null) {
            throw new DebeziumException("Cannot instantiate transformation '" + name + "'");
        }

        transformation.configure(config.subset(transformPrefix, true).asMap());

        if (isTraced(name)) {
            transformation = new TracingTransformation(name, transformation);
        }

        String predicateName = config.getString(transformPrefix + PREDICATE_SUFFIX);
        if (predicateName != null) {
            Boolean negate = config.getBoolean(transformPrefix + NEGATE_SUFFIX);
            Predicate<SourceRecord> predicate = this.predicates.getPredicate(predicateName);
            transformation = createPredicateTransformation(negate != null && negate, predicate, transformation);
        }

        return transformation;
    }

    private boolean isTraced(String name) {
        return tracingEnabled && (tracedAliases.isEmpty() || tracedAliases.contains(name));
    }

    private static boolean isOpenTelemetryAvailable() {
        try {
            Class.forName("io.opentelemetry.api.GlobalOpenTelemetry", false, Transformations.class.getClassLoader());
            return true;
        }
        catch (ClassNotFoundException | LinkageError e) {
            LOGGER.warn("Transformation tracing is enabled but the OpenTelemetry API is not available, transformations will not be traced");
            return false;
        }
    }

    public SourceRecord transform(SourceRecord record) {
        for (Transformation<SourceRecord> t : transforms) {
            record = t.apply(record);
            if (record == null) {
                break;
            }
        }
        return record;
    }

    private static Transformation<SourceRecord> createPredicateTransformation(boolean negate,
                                                                              Predicate<SourceRecord> predicate,
                                                                              Transformation<SourceRecord> transformation) {

        return new Transformation<>() {
            @Override
            public SourceRecord apply(SourceRecord sourceRecord) {
                if (negate ^ predicate.test(sourceRecord)) {
                    return transformation.apply(sourceRecord);
                }
                return sourceRecord;
            }

            @Override
            public ConfigDef config() {
                return null;
            }

            @Override
            public void close() {
                // predicate will be closed via the Predicates class
                try {
                    transformation.close();
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void configure(Map<String, ?> map) {
            }
        };
    }

    @Override
    public void close() throws IOException {

        for (Transformation<SourceRecord> t : transforms) {
            try {
                t.close();
            }
            catch (Exception e) {
                LOGGER.warn("Error while closing transformation", e);
            }
        }

        this.predicates.close();
    }
}
