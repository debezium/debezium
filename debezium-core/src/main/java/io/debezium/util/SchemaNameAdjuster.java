/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;

/**
 * A adjuster for the names of change data message schemas. Currently, this solely implements the rules required for
 * using these schemas in Avro messages. Avro <a href="http://avro.apache.org/docs/current/spec.html#names">rules for
 * schema fullnames</a> are as follows:
 *
 * <li>Each has a fullname that is composed of two parts; a name and a namespace. Equality of names is defined on the
 * fullname.</li>
 * <li>The name portion of a fullname, record field names, and enum symbols must start with a Latin letter or underscore
 * character (e.g., [A-Z,a-z,_]); and subsequent characters must be Latin alphanumeric or the underscore ({@code _})
 * characters (e.g., [A-Z,a-z,0-9,_]).</li>
 * <li>A namespace is a dot-separated sequence of such names.</li>
 * <li>Equality of names (including field names and enum symbols) as well as fullnames is case-sensitive.</li>
 * </ul>
 * <p>
 * A {@code SchemaNameAdjuster} can determine if the supplied fullname follows these Avro rules.
 *
 * @author Randall Hauch
 */
@FunctionalInterface
@ThreadSafe
public interface SchemaNameAdjuster {

    /**
     * Convert the proposed string to a valid Avro fullname, replacing all invalid characters with the underscore ('_')
     * character.
     *
     * @param proposedName the proposed fullname; may not be null
     * @return the valid fullname for Avro; never null
     */
    String adjust(String proposedName);

    /**
     * Function used to determine the replacement for a character that is not valid per Avro rules.
     */
    @FunctionalInterface
    @ThreadSafe
    public static interface ReplacementFunction {
        /**
         * Determine the replacement string for the invalid character.
         *
         * @param invalid the invalid character
         * @return the replacement string; may not be null
         */
        String replace(char invalid);
    }

    /**
     * Function used to report that an original value was replaced with an Avro-compatible string.
     */
    @FunctionalInterface
    @ThreadSafe
    public static interface ReplacementOccurred {
        /**
         * Accept that the original value was not Avro-compatible and was replaced.
         *
         * @param original the original value
         * @param replacement the replacement value
         * @param conflictsWithOriginal the other original value that resulted in the same replacement; may be null if there is
         *            no conflict
         */
        void accept(String original, String replacement, String conflictsWithOriginal);

        /**
         * Create a new function that calls this function only the first time it sees each unique original, and ignores
         * subsequent calls for originals it has already seen.
         *
         * @return the new function; never null
         */
        default ReplacementOccurred firstTimeOnly() {
            ReplacementOccurred delegate = this;
            Set<String> alreadySeen = Collections.newSetFromMap(new ConcurrentHashMap<>());
            Map<String, String> originalByReplacement = new ConcurrentHashMap<>();
            return (original, replacement, conflictsWith) -> {
                if (alreadySeen.add(original)) {
                    // We've not yet seen this original ...
                    String replacementsOriginal = originalByReplacement.put(replacement, original);
                    if (replacementsOriginal == null || original.equals(replacementsOriginal)) {
                        // We've not seen the replacement yet, so handle it ...
                        delegate.accept(original, replacement, null);
                    }
                    else {
                        // We've already seen the replacement with a different original, so this is a conflict ...
                        delegate.accept(original, replacement, replacementsOriginal);
                    }
                }
            };
        }

        /**
         * Create a new function that calls this function and then calls the next function.
         *
         * @param next the function to call after this function; may be null
         * @return the new function; never null
         */
        default ReplacementOccurred andThen(ReplacementOccurred next) {
            if (next == null) {
                return this;
            }
            return (original, replacement, conflictsWith) -> {
                accept(original, replacement, conflictsWith);
                next.accept(original, replacement, conflictsWith);
            };
        }
    }

    public static final SchemaNameAdjuster DEFAULT = create(LoggerFactory.getLogger(SchemaNameAdjuster.class));

    /**
     * Create a stateful Avro fullname adjuster that logs a warning the first time an invalid fullname is seen and replaced
     * with a valid fullname and throws an exception. This method replaces all invalid characters with the underscore character
     * ('_').
     *
     * @return the validator; never null
     */
    public static SchemaNameAdjuster defaultAdjuster() {
        return DEFAULT;
    }

    /**
     * Create a stateful Avro fullname adjuster that logs a warning the first time an invalid fullname is seen and replaced
     * with a valid fullname, and throws an error if the replacement conflicts with that of a different original. This method
     * replaces all invalid characters with the underscore character ('_').
     *
     * @param logger the logger to use; may not be null
     * @return the validator; never null
     */
    public static SchemaNameAdjuster create(Logger logger) {
        return create(logger, (original, replacement, conflict) -> {
            String msg = "The Kafka Connect schema name '" + original +
                    "' is not a valid Avro schema name and its replacement '" + replacement +
                    "' conflicts with another different schema '" + conflict + "'";
            throw new ConnectException(msg);
        });
    }

    /**
     * Create a stateful Avro fullname adjuster that logs a warning the first time an invalid fullname is seen and replaced
     * with a valid fullname. This method replaces all invalid characters with the underscore character ('_').
     *
     * @param logger the logger to use; may not be null
     * @param uponConflict the function to be called when there is a conflict and after that conflict is logged; may be null
     * @return the validator; never null
     */
    public static SchemaNameAdjuster create(Logger logger, ReplacementOccurred uponConflict) {
        ReplacementOccurred handler = (original, replacement, conflictsWith) -> {
            if (conflictsWith != null) {
                logger.error("The Kafka Connect schema name '{}' is not a valid Avro schema name and its replacement '{}' conflicts with another different schema '{}'",
                        original, replacement, conflictsWith);
                if (uponConflict != null) {
                    uponConflict.accept(original, replacement, conflictsWith);
                }
            }
            else {
                logger.warn("The Kafka Connect schema name '{}' is not a valid Avro schema name, so replacing with '{}'", original,
                        replacement);
            }
        };
        return create(handler.firstTimeOnly());
    }

    /**
     * Create a stateful Avro fullname adjuster that calls the supplied {@link ReplacementOccurred} function when an invalid
     * fullname is seen and replaced with a valid fullname. This method replaces all invalid characters with the underscore
     * character ('_').
     *
     * @param uponReplacement the function called each time the original fullname is replaced; may be null
     * @return the adjuster; never null
     */
    public static SchemaNameAdjuster create(ReplacementOccurred uponReplacement) {
        return create("_", uponReplacement);
    }

    /**
     * Create a stateful Avro fullname adjuster that calls the supplied {@link ReplacementOccurred} function when an invalid
     * fullname is seen and replaced with a valid fullname.
     *
     * @param replacement the character that should be used to replace all invalid characters
     * @param uponReplacement the function called each time the original fullname is replaced; may be null
     * @return the adjuster; never null
     */
    public static SchemaNameAdjuster create(char replacement, ReplacementOccurred uponReplacement) {
        String replacementStr = "" + replacement;
        return (original) -> validFullname(original, c -> replacementStr, uponReplacement);
    }

    /**
     * Create a stateful Avro fullname adjuster that calls the supplied {@link ReplacementOccurred} function when an invalid
     * fullname is seen and replaced with a valid fullname.
     *
     * @param replacement the character sequence that should be used to replace all invalid characters
     * @param uponReplacement the function called each time the original fullname is replaced; may be null
     * @return the adjuster; never null
     */
    public static SchemaNameAdjuster create(String replacement, ReplacementOccurred uponReplacement) {
        return (original) -> validFullname(original, c -> replacement, uponReplacement);
    }

    /**
     * Create a stateful Avro fullname adjuster that calls the supplied {@link ReplacementOccurred} function when an invalid
     * fullname is seen and replaced with a valid fullname.
     *
     * @param function the replacement function
     * @param uponReplacement the function called each time the original fullname is replaced; may be null
     * @return the adjuster; never null
     */
    public static SchemaNameAdjuster create(ReplacementFunction function, ReplacementOccurred uponReplacement) {
        return (original) -> validFullname(original, function, uponReplacement);
    }

    /**
     * Determine if the supplied string is a valid Avro namespace.
     *
     * @param fullname the name to be used as an Avro fullname; may not be null
     * @return {@code true} if the fullname satisfies Avro rules, or {@code false} otherwise
     */
    public static boolean isValidFullname(String fullname) {
        if (fullname.length() == 0) {
            return true;
        }
        char c = fullname.charAt(0);
        if (!isValidFullnameFirstCharacter(c)) {
            return false;
        }
        for (int i = 1; i != fullname.length(); ++i) {
            c = fullname.charAt(i);
            if (!isValidFullnameNonFirstCharacter(c)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Determine if the supplied character is a valid first character for Avro fullnames.
     *
     * @param c the character
     * @return {@code true} if the character is a valid first character of an Avro fullname, or {@code false} otherwise
     * @see #isValidFullname(String)
     */
    public static boolean isValidFullnameFirstCharacter(char c) {
        return c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
    }

    /**
     * Determine if the supplied character is a valid non-first character for Avro fullnames.
     *
     * @param c the character
     * @return {@code true} if the character is a valid non-first character of an Avro fullname, or {@code false} otherwise
     * @see #isValidFullname(String)
     */
    public static boolean isValidFullnameNonFirstCharacter(char c) {
        return c == '.' || isValidFullnameFirstCharacter(c) || (c >= '0' && c <= '9');
    }

    /**
     * Convert the proposed string to a valid Avro fullname, replacing all invalid characters with the underscore ('_') character.
     *
     * @param proposedName the proposed fullname; may not be null
     * @return the valid fullname for Avro; never null
     */
    public static String validFullname(String proposedName) {
        return validFullname(proposedName, "_");
    }

    /**
     * Convert the proposed string to a valid Avro fullname, replacing all invalid characters with the supplied string.
     *
     * @param proposedName the proposed fullname; may not be null
     * @param replacement the character sequence that should be used to replace all invalid characters
     * @return the valid fullname for Avro; never null
     */
    public static String validFullname(String proposedName, String replacement) {
        return validFullname(proposedName, c -> replacement);
    }

    /**
     * Convert the proposed string to a valid Avro fullname, using the supplied function to replace all invalid characters.
     *
     * @param proposedName the proposed fullname; may not be null
     * @param replacement the character sequence that should be used to replace all invalid characters
     * @return the valid fullname for Avro; never null
     */
    public static String validFullname(String proposedName, ReplacementFunction replacement) {
        return validFullname(proposedName, replacement, null);
    }

    /**
     * Convert the proposed string to a valid Avro fullname, using the supplied function to replace all invalid characters.
     *
     * @param proposedName the proposed fullname; may not be null
     * @param replacement the character sequence that should be used to replace all invalid characters
     * @param uponReplacement the function to be called every time the proposed name is invalid and replaced; may be null
     * @return the valid fullname for Avro; never null
     */
    public static String validFullname(String proposedName, ReplacementFunction replacement, ReplacementOccurred uponReplacement) {
        if (proposedName.length() == 0) {
            return proposedName;
        }
        StringBuilder sb = new StringBuilder();
        char c = proposedName.charAt(0);
        boolean changed = false;
        if (isValidFullnameFirstCharacter(c)) {
            sb.append(c);
        }
        else {
            sb.append(replacement.replace(c));
            changed = true;
        }
        for (int i = 1; i != proposedName.length(); ++i) {
            c = proposedName.charAt(i);
            if (isValidFullnameNonFirstCharacter(c)) {
                sb.append(c);
            }
            else {
                sb.append(replacement.replace(c));
                changed = true;
            }
        }
        if (!changed) {
            return proposedName;
        }
        // Otherwise, it is different ...
        String result = sb.toString();
        if (uponReplacement != null) {
            uponReplacement.accept(proposedName, result, null);
        }
        return result;
    }
}
