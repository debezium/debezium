/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.actions.snapshotting;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains information required for the snapshot
 */
public class SnapshotConfiguration {

    /**
     * this is a list of regular expressions
     */
    private List<String> dataCollections;

    private List<AdditionalCondition> additionalConditions;
    private String surrogateKey;

    public List<String> getDataCollections() {
        return dataCollections;
    }

    public List<AdditionalCondition> getAdditionalConditions() {
        return additionalConditions;
    }

    public String getSurrogateKey() {
        return surrogateKey;
    }

    public static final class Builder {
        private List<String> dataCollections;
        private final List<AdditionalCondition> additionalConditions = new ArrayList<>();
        private String surrogateKey;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder dataCollections(List<String> dataCollections) {
            this.dataCollections = dataCollections;
            return this;
        }

        public Builder addCondition(AdditionalCondition additionalCondition) {
            this.additionalConditions.add(additionalCondition);
            return this;
        }

        public Builder surrogateKey(String surrogateKey) {
            this.surrogateKey = surrogateKey;
            return this;
        }

        public SnapshotConfiguration build() {
            SnapshotConfiguration snapshotConfiguration = new SnapshotConfiguration();
            snapshotConfiguration.surrogateKey = this.surrogateKey;
            snapshotConfiguration.dataCollections = this.dataCollections;
            snapshotConfiguration.additionalConditions = this.additionalConditions;
            return snapshotConfiguration;
        }
    }
}
