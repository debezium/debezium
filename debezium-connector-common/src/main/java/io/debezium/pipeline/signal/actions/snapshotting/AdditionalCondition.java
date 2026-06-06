/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.actions.snapshotting;

import java.util.regex.Pattern;

/**
 * Contains filtering information for snapshot
 */
public class AdditionalCondition {

    /**
     * Tha data collection to which the filter applies
     */
    private Pattern dataCollection;

    /**
     * In case of an incremental snapshot specifies a condition based on the field(s) of the data collection(s).
     * For the blocking snapshot specifies the query statement for the connector to run on the data collection when it takes a snapshot
     */
    private String filter;

    public Pattern getDataCollection() {
        return dataCollection;
    }

    public String getFilter() {
        return filter;
    }

    @Override
    public String toString() {
        return "AdditionalCondition{" +
                "dataCollection=" + dataCollection +
                ", filter='" + filter + '\'' +
                '}';
    }

    public static final class AdditionalConditionBuilder {
        private Pattern dataCollection;
        private String filter;

        private AdditionalConditionBuilder() {
        }

        public static AdditionalConditionBuilder builder() {
            return new AdditionalConditionBuilder();
        }

        public AdditionalConditionBuilder dataCollection(Pattern dataCollection) {
            this.dataCollection = dataCollection;
            return this;
        }

        public AdditionalConditionBuilder filter(String filter) {
            this.filter = filter;
            return this;
        }

        public AdditionalCondition build() {
            AdditionalCondition additionalCondition = new AdditionalCondition();
            additionalCondition.filter = this.filter;
            additionalCondition.dataCollection = this.dataCollection;
            return additionalCondition;
        }
    }
}
