/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.StringJoiner;

/**
 * Interface that allows a class to more efficiently generate a string that is to be added to other strings via a
 * {@link StringJoiner}.
 * 
 * @author Randall Hauch
 */
@FunctionalInterface
public interface Stringifiable {

    /**
     * Used the supplied {@link StringJoiner} to record string representations of this object. The supplied joiner may concatenate
     * these strings together using custom delimiter strings.
     * 
     * @param joiner the string joiner instance that will accumulate the
     */
    void asString(StringJoiner joiner);

}
