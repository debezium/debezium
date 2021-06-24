/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;

/**
 * Composite class representing transformation chain.
 *
 * @author Jiri Pechanec
 *
 */
public class Transformations implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Transformations.class);

    private static final String TYPE_SUFFIX = ".type";

    private final Configuration config;
    private final List<Transformation<SourceRecord>> transforms = new ArrayList<>();

    public Transformations(Configuration config) {
        this.config = config;
        final String transformationList = config.getString(EmbeddedEngine.TRANSFORMS);
        if (transformationList == null) {
            return;
        }
        for (String transfName : transformationList.split(",")) {
            transfName = transfName.trim();
            final Transformation<SourceRecord> transformation = getTransformation(transfName);
            transformation.configure(config.subset(transformationConfigNamespace(transfName), true).asMap());
            transforms.add(transformation);
        }
    }

    private String transformationConfigNamespace(final String name) {
        return EmbeddedEngine.TRANSFORMS.name() + "." + name;
    }

    @SuppressWarnings("unchecked")
    private Transformation<SourceRecord> getTransformation(String name) {
        Transformation<SourceRecord> transformation = null;

        try {
            transformation = config.getInstance(EmbeddedEngine.TRANSFORMS.name() + "." + name + TYPE_SUFFIX, Transformation.class);
        }
        catch (Exception e) {
            throw new DebeziumException("Error while instantiating transformation '" + name + "'", e);
        }

        if (transformation == null) {
            throw new DebeziumException("Cannot instatiate transformation '" + name + "'");
        }

        return transformation;
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
    }
}
