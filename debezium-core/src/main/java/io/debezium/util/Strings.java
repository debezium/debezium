/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

import io.debezium.annotation.ThreadSafe;

/**
 * String-related utility methods.
 * 
 * @author Randall Hauch
 */
@ThreadSafe
public final class Strings {

    /**
     * Represents a predicate (boolean-valued function) of one character argument.
     */
    @FunctionalInterface
    public static interface CharacterPredicate {
        /**
         * Evaluates this predicate on the given character argument.
         *
         * @param c the input argument
         * @return {@code true} if the input argument matches the predicate, or {@code false} otherwise
         */
        boolean test(char c);
    }

    /**
     * Compare two {@link CharSequence} instances.
     * 
     * @param str1 the first character sequence; may be null
     * @param str2 the second character sequence; may be null
     * @return a negative integer if the first sequence is less than the second, zero if the sequence are equivalent (including if
     *         both are null), or a positive integer if the first sequence is greater than the second
     */
    public static int compareTo(CharSequence str1, CharSequence str2) {
        if (str1 == str2) return 0;
        if (str1 == null) return -1;
        if (str2 == null) return 1;
        return str1.toString().compareTo(str2.toString());
    }

    /**
     * Trim away any leading or trailing whitespace characters.
     * <p>
     * This is semantically equivalent to {@link String#trim()} but instead uses {@link #trim(String, CharacterPredicate)}.
     * 
     * @param str the string to be trimmed; may not be null
     * @return the trimmed string; never null
     * @see #trim(String,CharacterPredicate)
     */
    public static String trim(String str) {
        return trim(str, c -> c <= ' ');    // same logic as String.trim()
    }

    /**
     * Trim away any leading or trailing characters that satisfy the supplied predicate
     * 
     * @param str the string to be trimmed; may not be null
     * @param predicate the predicate function; may not be null
     * @return the trimmed string; never null
     * @see #trim(String)
     */
    public static String trim(String str, CharacterPredicate predicate) {
        int len = str.length();
        if (len == 0) return str;
        int st = 0;
        while ((st < len) && predicate.test(str.charAt(st))) {
            st++;
        }
        while ((st < len) && predicate.test(str.charAt(len - 1))) {
            len--;
        }
        return ((st > 0) || (len < str.length())) ? str.substring(st, len) : str;
    }

    /**
     * Create a new string containing the specified character repeated a specific number of times.
     * 
     * @param charToRepeat the character to repeat
     * @param numberOfRepeats the number of times the character is to repeat in the result; must be greater than 0
     * @return the resulting string
     */
    public static String createString( final char charToRepeat,
                                       int numberOfRepeats ) {
        assert numberOfRepeats >= 0;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numberOfRepeats; ++i) {
            sb.append(charToRepeat);
        }
        return sb.toString();
    }

    /**
     * Set the length of the string, padding with the supplied character if the supplied string is shorter than desired, or
     * truncating the string if it is longer than desired. Unlike {@link #justifyLeft(String, int, char)}, this method does not
     * remove leading and trailing whitespace.
     * 
     * @param original the string for which the length is to be set; may not be null
     * @param length the desired length; must be positive
     * @param padChar the character to use for padding, if the supplied string is not long enough
     * @return the string of the desired length
     * @see #justifyLeft(String, int, char)
     */
    public static String setLength( String original,
                                    int length,
                                    char padChar ) {
        return justifyLeft(original, length, padChar, false);
    }

    public static enum Justify {
        LEFT,
        RIGHT,
        CENTER;
    }

    /**
     * Justify the contents of the string.
     * 
     * @param justify the way in which the string is to be justified
     * @param str the string to be right justified; if null, an empty string is used
     * @param width the desired width of the string; must be positive
     * @param padWithChar the character to use for padding, if needed
     * @return the right justified string
     */
    public static String justify( Justify justify,
                                  String str,
                                  final int width,
                                  char padWithChar ) {
        switch (justify) {
            case LEFT:
                return justifyLeft(str, width, padWithChar);
            case RIGHT:
                return justifyRight(str, width, padWithChar);
            case CENTER:
                return justifyCenter(str, width, padWithChar);
        }
        assert false;
        return null;
    }

    /**
     * Right justify the contents of the string, ensuring that the string ends at the last character. If the supplied string is
     * longer than the desired width, the leading characters are removed so that the last character in the supplied string at the
     * last position. If the supplied string is shorter than the desired width, the padding character is inserted one or more
     * times such that the last character in the supplied string appears as the last character in the resulting string and that
     * the length matches that specified.
     * 
     * @param str the string to be right justified; if null, an empty string is used
     * @param width the desired width of the string; must be positive
     * @param padWithChar the character to use for padding, if needed
     * @return the right justified string
     */
    public static String justifyRight( String str,
                                       final int width,
                                       char padWithChar ) {
        assert width > 0;
        // Trim the leading and trailing whitespace ...
        str = str != null ? str.trim() : "";

        final int length = str.length();
        int addChars = width - length;
        if (addChars < 0) {
            // truncate the first characters, keep the last
            return str.subSequence(length - width, length).toString();
        }
        // Prepend the whitespace ...
        final StringBuilder sb = new StringBuilder();
        while (addChars > 0) {
            sb.append(padWithChar);
            --addChars;
        }

        // Write the content ...
        sb.append(str);
        return sb.toString();
    }

    /**
     * Left justify the contents of the string, ensuring that the supplied string begins at the first character and that the
     * resulting string is of the desired length. If the supplied string is longer than the desired width, it is truncated to the
     * specified length. If the supplied string is shorter than the desired width, the padding character is added to the end of
     * the string one or more times such that the length is that specified. All leading and trailing whitespace is removed.
     * 
     * @param str the string to be left justified; if null, an empty string is used
     * @param width the desired width of the string; must be positive
     * @param padWithChar the character to use for padding, if needed
     * @return the left justified string
     * @see #setLength(String, int, char)
     */
    public static String justifyLeft( String str,
                                      final int width,
                                      char padWithChar ) {
        return justifyLeft(str, width, padWithChar, true);
    }

    protected static String justifyLeft( String str,
                                         final int width,
                                         char padWithChar,
                                         boolean trimWhitespace ) {
        // Trim the leading and trailing whitespace ...
        str = str != null ? (trimWhitespace ? str.trim() : str) : "";

        int addChars = width - str.length();
        if (addChars < 0) {
            // truncate
            return str.subSequence(0, width).toString();
        }
        // Write the content ...
        final StringBuilder sb = new StringBuilder();
        sb.append(str);

        // Append the whitespace ...
        while (addChars > 0) {
            sb.append(padWithChar);
            --addChars;
        }

        return sb.toString();
    }

    /**
     * Center the contents of the string. If the supplied string is longer than the desired width, it is truncated to the
     * specified length. If the supplied string is shorter than the desired width, padding characters are added to the beginning
     * and end of the string such that the length is that specified; one additional padding character is prepended if required.
     * All leading and trailing whitespace is removed before centering.
     * 
     * @param str the string to be left justified; if null, an empty string is used
     * @param width the desired width of the string; must be positive
     * @param padWithChar the character to use for padding, if needed
     * @return the left justified string
     * @see #setLength(String, int, char)
     */
    public static String justifyCenter( String str,
                                        final int width,
                                        char padWithChar ) {
        // Trim the leading and trailing whitespace ...
        str = str != null ? str.trim() : "";

        int addChars = width - str.length();
        if (addChars < 0) {
            // truncate
            return str.subSequence(0, width).toString();
        }
        // Write the content ...
        int prependNumber = addChars / 2;
        int appendNumber = prependNumber;
        if ((prependNumber + appendNumber) != addChars) {
            ++prependNumber;
        }

        final StringBuilder sb = new StringBuilder();

        // Prepend the pad character(s) ...
        while (prependNumber > 0) {
            sb.append(padWithChar);
            --prependNumber;
        }

        // Add the actual content
        sb.append(str);

        // Append the pad character(s) ...
        while (appendNumber > 0) {
            sb.append(padWithChar);
            --appendNumber;
        }

        return sb.toString();
    }

    /**
     * Get the stack trace of the supplied exception.
     * 
     * @param throwable the exception for which the stack trace is to be returned
     * @return the stack trace, or null if the supplied exception is null
     */
    public static String getStackTrace(Throwable throwable) {
        if (throwable == null) return null;
        final ByteArrayOutputStream bas = new ByteArrayOutputStream();
        final PrintWriter pw = new PrintWriter(bas);
        throwable.printStackTrace(pw);
        pw.close();
        return bas.toString();
    }

    /**
     * Parse the supplied string as a integer value.
     * 
     * @param value the string representation of a integer value
     * @param defaultValue the value to return if the string value is null or cannot be parsed as an int
     * @return the int value
     */
    public static int asInt(String value, int defaultValue) {
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
            }
        }
        return defaultValue;
    }

    /**
     * Parse the supplied string as a long value.
     * 
     * @param value the string representation of a long value
     * @param defaultValue the value to return if the string value is null or cannot be parsed as a long
     * @return the long value
     */
    public static long asLong(String value, long defaultValue) {
        if (value != null) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
            }
        }
        return defaultValue;
    }
    
    /**
     * Parse the supplied string as a double value.
     * 
     * @param value the string representation of a double value
     * @param defaultValue the value to return if the string value is null or cannot be parsed as a double
     * @return the double value
     */
    public static double asDouble(String value, double defaultValue) {
        if (value != null) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
            }
        }
        return defaultValue;
    }

    /**
     * Parse the supplied string as a boolean value.
     * 
     * @param value the string representation of a boolean value
     * @param defaultValue the value to return if the string value is null or cannot be parsed as a boolean
     * @return the boolean value
     */
    public static boolean asBoolean(String value, boolean defaultValue) {
        if (value != null) {
            try {
                return Boolean.parseBoolean(value);
            } catch (NumberFormatException e) {
            }
        }
        return defaultValue;
    }
    
    private Strings() {
    }
}
