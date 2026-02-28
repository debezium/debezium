/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import java.util.Properties;

import io.debezium.common.annotation.Incubating;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * An abstract unicode converter topic naming strategy implementation of {@link TopicNamingStrategy}.
 * Any non encodable character or underscore would be encoded via _uxxxx sequence in the same way as Java works,
 * and the underscore is an escape sequence like backslash in Java.
 *
 * @author Harvey Yue
 */
@Incubating
public abstract class AbstractUnicodeTopicNamingStrategy extends AbstractTopicNamingStrategy<DataCollectionId> {

    public AbstractUnicodeTopicNamingStrategy(Properties props) {
        super(props);
        replacement = new UnicodeReplacementFunction();
    }

    // The underscore is an escape sequence like backslash in Java, so need to convert it to corresponding unicode.
    @Override
    public boolean isValidCharacter(char c) {
        return c == '.' || c == '-' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
    }
}
