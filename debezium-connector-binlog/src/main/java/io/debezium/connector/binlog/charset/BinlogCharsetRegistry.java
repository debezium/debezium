/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.charset;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import io.debezium.service.Service;

/**
 * Contract for handling binlog-based character set features
 *
 * @author Chris Cranford
 */
public interface BinlogCharsetRegistry extends Service {
    /**
     * Get the size of the character set registry map.
     * @return the map size
     */
    int getCharsetMapSize();

    /**
     * Get the collation name for a collation index.
     *
     * @param collationIndex the collation index, should not be {@code null}
     * @return the collation name or {@code null} if not found or index is invalid
     */
    String getCollationNameForCollationIndex(Integer collationIndex);

    /**
     * Get the collation name for the collation index
     *
     * @param collationIndex the collation index, should not be {@code null}
     * @return the collation name or {@code null} if not found or index is invalid
     */
    String getCharsetNameForCollationIndex(Integer collationIndex);

    /**
     * Get the Java encoding for a database character set name.
     *
     * @param charSetName the database character set name
     * @return the java encoding for the character set name
     */
    String getJavaEncodingForCharSet(String charSetName);

    /**
     * Resolves the character set implied by a collation name, following the MySQL and MariaDB naming rule
     * {@code <charset>_<suffix>}. Collation names without a character set prefix, such as MariaDB's
     * {@code uca1400_ai_ci}, do not change the character set and return {@code null}.
     *
     * @param collationName the collation name; may be null
     * @return the character set name, or {@code null} if the collation does not imply a character set
     */
    default String getCharsetNameForCollation(String collationName) {
        if (collationName == null || collationName.isEmpty()) {
            return null;
        }
        final var name = collationName.toLowerCase(Locale.ROOT);
        final Set<String> charsetNames = new HashSet<>();
        for (int index = 0; index < getCharsetMapSize(); index++) {
            if (name.equals(getCollationNameForCollationIndex(index))) {
                return getCharsetNameForCollationIndex(index);
            }
            final var charsetName = getCharsetNameForCollationIndex(index);
            if (charsetName != null) {
                charsetNames.add(charsetName.toLowerCase(Locale.ROOT));
            }
        }
        if (charsetNames.contains(name)) {
            // e.g. the 'binary' collation of the 'binary' character set
            return name;
        }
        final var separator = name.indexOf('_');
        if (separator <= 0) {
            return null;
        }
        final var prefix = name.substring(0, separator);
        if ("utf8".equals(prefix)) {
            // 'utf8' is an alias of 'utf8mb3'
            return charsetNames.contains("utf8mb3") ? "utf8mb3" : "utf8";
        }
        return charsetNames.contains(prefix) ? prefix : null;
    }
}
