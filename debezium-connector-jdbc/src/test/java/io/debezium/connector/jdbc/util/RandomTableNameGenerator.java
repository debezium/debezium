/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.util;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Simple class used to generate random table/object names.
 *
 * @author Chris Cranford
 */
public class RandomTableNameGenerator {

    private static final String LEXICON = "abcdefghijklmnopqrstuvwxyz0123456789";
    private static final Random RANDOM = new Random();

    private final Set<String> names = new HashSet<>();

    public String randomName() {
        return randomName(RANDOM.nextInt(5) + 5);
    }

    public String randomName(int length) {
        StringBuilder builder = new StringBuilder();
        while (builder.toString().length() == 0) {
            // Some databases require object names to begin with a character, so we enforce that
            builder.append(LEXICON.charAt(RANDOM.nextInt(26)));
            for (int i = 0; i < length - 1; ++i) {
                builder.append(LEXICON.charAt(RANDOM.nextInt(LEXICON.length())));
            }
            if (names.contains(builder.toString())) {
                builder = new StringBuilder();
            }
        }
        names.add(builder.toString());
        return builder.toString();
    }
}
