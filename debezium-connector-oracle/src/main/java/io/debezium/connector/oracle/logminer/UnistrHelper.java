/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.util.regex.Pattern;

/**
 * A utility/helper class to support decoding Oracle Unicode String function values, {@code UNISTR}.
 *
 * @author Chris Cranford
 */
public class UnistrHelper {

    private static final String UNITSTR_FUNCTION_START = "UNISTR('";
    private static final String UNISTR_FUNCTION_END = "')";
    private static final Pattern CONCATENATION_PATTERN = Pattern.compile("\\|\\|");

    public static boolean isUnistrFunction(String data) {
        return data != null && data.startsWith(UNITSTR_FUNCTION_START) && data.endsWith(UNISTR_FUNCTION_END);
    }

    public static String convert(String data) {
        if (data == null || data.length() == 0) {
            return data;
        }

        // If the UNISTR contains an escaped single-quote, we should clean that up first.
        if (data.contains("''")) {
            data = data.replaceAll("''", "'");
        }

        // Multiple UNISTR function calls maybe concatenated together using "||".
        // We split the values into their respective parts before parsing each one separately.
        final String[] parts = CONCATENATION_PATTERN.split(data);

        // Iterate each part and if the part is a UNISTR function call, decode it
        // Append each part's value to the final result
        final StringBuilder result = new StringBuilder();
        for (final String part : parts) {
            final String trimmedPart = part.trim();
            if (isUnistrFunction(trimmedPart)) {
                result.append(decode(trimmedPart.substring(8, trimmedPart.length() - 2)));
            }
            else {
                result.append(data);
            }
        }
        return result.toString();
    }

    private static String decode(String value) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < value.length(); ++i) {
            final char c = value.charAt(i);
            if (c == '\\') {
                if (value.length() >= (i + 4)) {
                    // Read next 4 character hex and convert to character.
                    result.append(Character.toChars(Integer.parseInt(value.substring(i + 1, i + 5), 16)));
                    i += 4;
                    continue;
                }
            }
            result.append(c);
        }
        return result.toString();
    }
}
