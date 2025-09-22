/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import io.debezium.annotation.ThreadSafe;
import io.debezium.text.ParsingException;
import io.debezium.text.TokenStream;
import io.debezium.text.TokenStream.CharacterStream;
import io.debezium.text.TokenStream.Tokenizer;
import io.debezium.text.TokenStream.Tokens;

/**
 * String-related utility methods.
 *
 * @author Randall Hauch
 * @author Horia Chiorean
 */
@ThreadSafe
public final class Strings {

    private static final Pattern TIME_PATTERN = Pattern.compile("([0-9]*):([0-9]*):([0-9]*)(\\.([0-9]*))?");

    /**
     * Generate the set of values that are included in the list.
     *
     * @param input the input string
     * @param splitter the function that splits the input into multiple items; may not be null
     * @param factory the factory for creating string items into filter matches; may not be null
     * @param trim specifies whether each input item is trimmed before being added to the returned collection
     * @return the set of objects included in the list; never null
     */
    private static <T> Set<T> setOf(String input, Function<String, String[]> splitter, Function<String, T> factory, boolean trim) {
        if (input == null) {
            return Collections.emptySet();
        }
        Set<T> matches = new HashSet<>();
        for (String item : splitter.apply(input)) {
            T obj = factory.apply(trim ? item.trim() : item);
            if (obj != null) {
                matches.add(obj);
            }
        }
        return matches;
    }

    /**
     * Generate the set of values that are included in the list.
     *
     * @param input the input string
     * @param splitter the function that splits the input into multiple items; may not be null
     * @param factory the factory for creating string items into filter matches; may not be null
     * @return the set of objects included in the list; never null
     */
    public static <T> Set<T> setOf(String input, Function<String, String[]> splitter, Function<String, T> factory) {
        return setOf(input, splitter, factory, false);
    }

    /**
     * Generate the set of values that are included in the list, with each element trimmed.
     *
     * @param input the input string
     * @param splitter the function that splits the input into multiple items; may not be null
     * @param factory the factory for creating string items into filter matches; may not be null
     * @return the set of objects included in the list; never null
     */
    public static <T> Set<T> setOfTrimmed(String input, Function<String, String[]> splitter, Function<String, T> factory) {
        return setOf(input, splitter, factory, true);
    }

    /**
     * Generate the list of values that are included in the list.
     *
     * @param input the input string
     * @param splitter the function that splits the input into multiple items; may not be null
     * @param factory the factory for creating string items into filter matches; may not be null
     * @param trim specifies whether each input item is trimmed before being added to the returned collection
     * @return the list of objects included in the list; never null
     */
    private static <T> List<T> listOf(String input, Function<String, String[]> splitter, Function<String, T> factory, boolean trim) {
        if (input == null) {
            return Collections.emptyList();
        }
        List<T> matches = new ArrayList<T>();
        for (String item : splitter.apply(input)) {
            T obj = factory.apply(trim ? item.trim() : item);
            if (obj != null) {
                matches.add(obj);
            }
        }
        return matches;
    }

    /**
     * Generate the list of values that are included in the list.
     *
     * @param input the input string
     * @param splitter the function that splits the input into multiple items; may not be null
     * @param factory the factory for creating string items into filter matches; may not be null
     * @return the list of objects included in the list; never null
     */
    public static <T> List<T> listOf(String input, Function<String, String[]> splitter, Function<String, T> factory) {
        return listOf(input, splitter, factory, false);
    }

    /**
     * Generate the list of values that are included in the list, with each element trimmed.
     *
     * @param input the input string
     * @param splitter the function that splits the input into multiple items; may not be null
     * @param factory the factory for creating string items into filter matches; may not be null
     * @return the list of objects included in the list; never null
     */
    public static <T> List<T> listOfTrimmed(String input, Function<String, String[]> splitter, Function<String, T> factory) {
        return listOf(input, splitter, factory, true);
    }

    /**
     * Generate the list of values that are included in the list separated by commas, with each element trimmed.
     *
     * @param input the input string
     * @param factory the factory for creating string items into filter matches; may not be null
     * @return the list of objects included in the list; never null
     */
    public static <T> List<T> listOfTrimmed(String input, Function<String, T> factory) {
        return listOfTrimmed(input, ',', factory);
    }

    /**
     * Generate the list of values that are included in the list delimited by the given delimiter.
     *
     * @param input the input string
     * @param delimiter the character used to delimit the items in the input
     * @param factory the factory for creating string items into filter matches; may not be null
     * @return the list of objects included in the list; never null
     */
    public static <T> List<T> listOfTrimmed(String input, char delimiter, Function<String, T> factory) {
        return listOf(input, (str) -> str.split("[" + delimiter + "]"), factory);
    }

    /**
     * Generate the set of values that are included in the list delimited by the given delimiter.
     *
     * @param input the input string
     * @param delimiter the character used to delimit the items in the input
     * @param factory the factory for creating string items into filter matches; may not be null
     * @return the set of objects included in the list; never null
     */
    public static <T> Set<T> setOf(String input, char delimiter, Function<String, T> factory) {
        return setOf(input, (str) -> str.split("[" + delimiter + "]"), factory);
    }

    /**
     * Generate the set of values that are included in the list delimited by the given delimiter, with each element trimmed.
     *
     * @param input the input string
     * @param delimiter the character used to delimit the items in the input
     * @param factory the factory for creating string items into filter matches; may not be null
     * @return the set of objects included in the list; never null
     */
    public static <T> Set<T> setOfTrimmed(String input, char delimiter, Function<String, T> factory) {
        return setOfTrimmed(input, (str) -> str.split("[" + delimiter + "]"), factory);
    }

    /**
     * Generate the set of values that are included in the list separated by commas.
     *
     * @param input the input string
     * @param factory the factory for creating string items into filter matches; may not be null
     * @return the set of objects included in the list; never null
     */
    public static <T> Set<T> setOf(String input, Function<String, T> factory) {
        return setOf(input, ',', factory);
    }

    /**
     * Generate the set of values that are included in the list separated by commas, with each element trimmed.
     *
     * @param input the input string
     * @param factory the factory for creating string items into filter matches; may not be null
     * @return the set of objects included in the list; never null
     */
    public static <T> Set<T> setOfTrimmed(String input, Function<String, T> factory) {
        return setOfTrimmed(input, ',', factory);
    }

    /**
     * Generate the set of regular expression {@link Pattern}s that are specified in the string containing comma-separated
     * regular expressions.
     *
     * @param input the input string with comma-separated regular expressions. Comma can be escaped with backslash.
     * @return the set of regular expression {@link Pattern}s included within the given string; never null
     * @throws PatternSyntaxException if the input includes an invalid regular expression
     */
    public static Set<Pattern> setOfRegex(String input, int regexFlags) {
        return setOf(input, RegExSplitter::split, (str) -> Pattern.compile(str, regexFlags));
    }

    /**
     * Generate the set of regular expression {@link Pattern}s that are specified in the string containing comma-separated
     * regular expressions.
     *
     * @param input the input string with comma-separated regular expressions. Comma can be escaped with backslash.
     * @return the set of regular expression {@link Pattern}s included within the given string; never null
     * @throws PatternSyntaxException if the input includes an invalid regular expression
     */
    public static Set<Pattern> setOfRegex(String input) {
        return setOf(input, RegExSplitter::split, Pattern::compile);
    }

    /**
     * Generate the set of regular expression {@link Pattern}s that are specified in the string containing comma-separated
     * regular expressions.
     *
     * @param input the input string with comma-separated regular expressions. Comma can be escaped with backslash.
     * @param regexFlags the flags for {@link Pattern#compile(String, int) compiling regular expressions}
     * @return the list of regular expression {@link Pattern}s included in the list; never null
     * @throws PatternSyntaxException if the input includes an invalid regular expression
     * @throws IllegalArgumentException if bit values other than those corresponding to the defined
     *             match flags are set in {@code regexFlags}
     */
    public static List<Pattern> listOfRegex(String input, int regexFlags) {
        return listOf(input, RegExSplitter::split, (str) -> Pattern.compile(str, regexFlags));
    }

    /**
     * Represents a predicate (boolean-valued function) of one character argument.
     */
    @FunctionalInterface
    public interface CharacterPredicate {
        /**
         * Evaluates this predicate on the given character argument.
         *
         * @param c the input argument
         * @return {@code true} if the input argument matches the predicate, or {@code false} otherwise
         */
        boolean test(char c);
    }

    /**
     * Split the supplied content into lines, returning each line as an element in the returned list.
     *
     * @param content the string content that is to be split
     * @return the list of lines; never null but may be an empty (unmodifiable) list if the supplied content is null or empty
     */
    public static List<String> splitLines(final String content) {
        if (content == null || content.length() == 0) {
            return Collections.emptyList();
        }
        String[] lines = content.split("[\\r]?\\n");
        return Arrays.asList(lines);
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
        if (str1 == str2) {
            return 0;
        }
        if (str1 == null) {
            return -1;
        }
        if (str2 == null) {
            return 1;
        }
        return str1.toString().compareTo(str2.toString());
    }

    /**
     * Check whether the two {@link String} instances are equal ignoring case.
     *
     * @param str1 the first character sequence; may be null
     * @param str2 the second character sequence; may be null
     * @return {@code true} if both are null or if the two strings are equal to each other ignoring case, or {@code false}
     *         otherwise
     */
    public static boolean equalsIgnoreCase(String str1, String str2) {
        if (str1 == str2) {
            return true;
        }
        if (str1 == null) {
            return str2 == null;
        }
        return str1.equalsIgnoreCase(str2);
    }

    /**
     * Returns a new String composed of the supplied integer values joined together
     * with a copy of the specified {@code delimiter}.
     *
     * @param delimiter the delimiter that separates each element
     * @param values the values to join together.
     * @return a new {@code String} that is composed of the {@code elements} separated by the {@code delimiter}
     *
     * @throws NullPointerException If {@code delimiter} or {@code elements} is {@code null}
     * @see java.lang.String#join
     */
    public static String join(CharSequence delimiter, int[] values) {
        Objects.requireNonNull(delimiter);
        Objects.requireNonNull(values);
        if (values.length == 0) {
            return "";
        }
        if (values.length == 1) {
            return Integer.toString(values[0]);
        }
        StringBuilder sb = new StringBuilder();
        sb.append(values[0]);
        for (int i = 1; i != values.length; ++i) {
            sb.append(delimiter);
            sb.append(values[i]);
        }
        return sb.toString();
    }

    /**
     * Returns a new String composed of the supplied values joined together with a copy of the specified {@code delimiter}.
     * All {@code null} values are simply ignored.
     *
     * @param delimiter the delimiter that separates each element
     * @param values the values to join together.
     * @return a new {@code String} that is composed of the {@code elements} separated by the {@code delimiter}
     *
     * @throws NullPointerException If {@code delimiter} or {@code elements} is {@code null}
     * @see java.lang.String#join
     */
    public static <T> String join(CharSequence delimiter, Iterable<T> values) {
        return join(delimiter, values, v -> {
            return v != null ? v.toString() : null;
        });
    }

    /**
     * Returns a new String composed of the supplied values joined together with a copy of the specified {@code delimiter}.
     *
     * @param delimiter the delimiter that separates each element
     * @param values the values to join together.
     * @param conversion the function that converts the supplied values into strings, or returns {@code null} if the value
     *            is to be excluded
     * @return a new {@code String} that is composed of the {@code elements} separated by the {@code delimiter}
     *
     * @throws NullPointerException If {@code delimiter} or {@code elements} is {@code null}
     * @see java.lang.String#join
     */
    public static <T> String join(CharSequence delimiter, Iterable<T> values, Function<T, String> conversion) {
        Objects.requireNonNull(delimiter);
        Objects.requireNonNull(values);
        Iterator<T> iter = values.iterator();
        if (!iter.hasNext()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        String first = conversion.apply(iter.next());
        boolean delimit = false;
        if (first != null) {
            sb.append(first);
            delimit = true;
        }
        while (iter.hasNext()) {
            String next = conversion.apply(iter.next());
            if (next != null) {
                if (delimit) {
                    sb.append(delimiter);
                }
                sb.append(next);
                delimit = true;
            }
        }
        return sb.toString();
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
        return trim(str, c -> c <= ' '); // same logic as String.trim()
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
        if (len == 0) {
            return str;
        }
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
    public static String createString(final char charToRepeat,
                                      int numberOfRepeats) {
        assert numberOfRepeats >= 0;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numberOfRepeats; ++i) {
            sb.append(charToRepeat);
        }
        return sb.toString();
    }

    /**
     * Pad the string with the specific character to ensure the string is at least the specified length.
     *
     * @param original the string to be padded; may not be null
     * @param length the minimum desired length; must be positive
     * @param padChar the character to use for padding, if the supplied string is not long enough
     * @return the padded string of the desired length
     * @see #justifyLeft(String, int, char)
     */
    public static String pad(String original,
                             int length,
                             char padChar) {
        if (original.length() >= length) {
            return original;
        }
        StringBuilder sb = new StringBuilder(original);
        while (sb.length() < length) {
            sb.append(padChar);
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
    public static String setLength(String original,
                                   int length,
                                   char padChar) {
        return justifyLeft(original, length, padChar, false);
    }

    public enum Justify {
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
    public static String justify(Justify justify,
                                 String str,
                                 final int width,
                                 char padWithChar) {
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
    public static String justifyRight(String str,
                                      final int width,
                                      char padWithChar) {
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
    public static String justifyLeft(String str,
                                     final int width,
                                     char padWithChar) {
        return justifyLeft(str, width, padWithChar, true);
    }

    protected static String justifyLeft(String str,
                                        final int width,
                                        char padWithChar,
                                        boolean trimWhitespace) {
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
    public static String justifyCenter(String str,
                                       final int width,
                                       char padWithChar) {
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
        if (throwable == null) {
            return null;
        }
        final ByteArrayOutputStream bas = new ByteArrayOutputStream();
        final PrintWriter pw = new PrintWriter(bas);
        throwable.printStackTrace(pw);
        pw.close();
        return bas.toString();
    }

    /**
     * Parse the supplied string as a number.
     *
     * @param value the string representation of a integer value
     * @return the number, or {@code null} if the value is not a number
     */
    public static Number asNumber(String value) {
        return asNumber(value, null);
    }

    /**
     * Parse the supplied string as a number.
     *
     * @param value the string representation of a integer value
     * @param defaultValueProvider the function that returns a value to be used when the string value is null or cannot be parsed
     *            as a number; may be null if no default value is to be used
     * @return the number, or {@code null} if the value is not a number and no default value is supplied
     */
    public static Number asNumber(String value, Supplier<Number> defaultValueProvider) {
        if (value != null) {
            try {
                return Short.valueOf(value);
            }
            catch (NumberFormatException e1) {
                try {
                    return Integer.valueOf(value);
                }
                catch (NumberFormatException e2) {
                    try {
                        return Long.valueOf(value);
                    }
                    catch (NumberFormatException e3) {
                        try {
                            return Float.valueOf(value);
                        }
                        catch (NumberFormatException e4) {
                            try {
                                return Double.valueOf(value);
                            }
                            catch (NumberFormatException e5) {
                                try {
                                    return new BigInteger(value);
                                }
                                catch (NumberFormatException e6) {
                                    try {
                                        return new BigDecimal(value);
                                    }
                                    catch (NumberFormatException e7) {
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return defaultValueProvider != null ? defaultValueProvider.get() : null;
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
            }
            catch (NumberFormatException e) {
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
            }
            catch (NumberFormatException e) {
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
            }
            catch (NumberFormatException e) {
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
            }
            catch (NumberFormatException e) {
            }
        }
        return defaultValue;
    }

    /**
     * Converts the given string (in the format 00:00:00(.0*)) into a {@link Duration}.
     *
     * @return the given value as {@code Duration} or {@code null} if {@code null} was passed.
     */
    public static Duration asDuration(String timeString) {
        if (timeString == null) {
            return null;
        }
        Matcher matcher = TIME_PATTERN.matcher(timeString);

        if (!matcher.matches()) {
            throw new RuntimeException("Unexpected format for TIME column: " + timeString);
        }

        long hours = Long.parseLong(matcher.group(1));
        long minutes = Long.parseLong(matcher.group(2));
        long seconds = Long.parseLong(matcher.group(3));
        long nanoSeconds = 0;
        String microSecondsString = matcher.group(5);
        if (microSecondsString != null) {
            nanoSeconds = Long.parseLong(Strings.justifyLeft(microSecondsString, 9, '0'));
        }

        if (hours >= 0) {
            return Duration.ofHours(hours)
                    .plusMinutes(minutes)
                    .plusSeconds(seconds)
                    .plusNanos(nanoSeconds);
        }
        else {
            return Duration.ofHours(hours)
                    .minusMinutes(minutes)
                    .minusSeconds(seconds)
                    .minusNanos(nanoSeconds);
        }
    }

    /**
     * For the given duration in milliseconds, obtain a readable representation of the form {@code HHH:MM:SS.mmm}, where
     * <dl>
     * <dt>HHH</dt>
     * <dd>is the number of hours written in at least 2 digits (e.g., "03")</dd>
     * <dt>MM</dt>
     * <dd>is the number of minutes written in 2 digits (e.g., "05")</dd>
     * <dt>SS</dt>
     * <dd>is the number of seconds written in 2 digits (e.g., "09")</dd>
     * <dt>mmm</dt>
     * <dd>is the fractional part of seconds, written with 1-3 digits (any trailing zeros are dropped)</dd>
     * </dl>
     *
     * @param durationInMillis the duration in milliseconds
     * @return the readable duration.
     */
    public static String duration(long durationInMillis) {
        long seconds = durationInMillis / 1000;
        long s = seconds % 60;
        long m = (seconds / 60) % 60;
        long h = (seconds / (60 * 60));
        long q = durationInMillis % 1000;

        StringBuilder result = new StringBuilder(15);

        if (h < 10) {
            result.append("0");
        }

        result.append(h).append(":");

        if (m < 10) {
            result.append("0");
        }

        result.append(m).append(":");

        if (s < 10) {
            result.append("0");
        }

        result.append(s).append(".");

        if (q == 0) {
            result.append("0");
            return result.toString();
        }
        else if (q < 10) {
            result.append("00");
        }
        else if (q < 100) {
            result.append("0");
        }

        result.append(q);

        int length = result.length();

        if (result.charAt(length - 1) == '0') {
            if (result.charAt(length - 2) == '0') {
                return result.substring(0, length - 2);
            }
            else {
                return result.substring(0, length - 1);
            }
        }
        else {
            return result.toString();
        }
    }

    /**
     * Obtain a function that will replace variables in the supplied value with values from the supplied lookup function.
     * <p>
     * Variables may appear anywhere within a string value, and multiple variables can be used within the same value. Variables
     * take the form:
     *
     * <pre>
     *    variable := '${' variableNames [ ':' defaultValue ] '}'
     *    variableNames := variableName [ ',' variableNames ]
     *    variableName := // any characters except ',' and ':' and '}'
     *    defaultValue := // any characters except '}'
     * </pre>
     *
     * Note that <i>variableName</i> is the name used to look up a the property.
     * </p>
     * Notice that the syntax supports multiple <i>variables</i>. The logic will process the <i>variables</i> from let to right,
     * until an existing property is found. And at that point, it will stop and will not attempt to find values for the other
     * <i>variables</i>.
     * <p>
     *
     * @param replacementsByVariableName the function used to find the replacements for variable names; may not be null
     * @return the function that will replace variables in supplied strings; never null
     */
    public static Function<String, String> replaceVariablesWith(Function<String, String> replacementsByVariableName) {
        return (value) -> {
            return replaceVariables(value, replacementsByVariableName);
        };
    }

    private static final String CURLY_PREFIX = "${";
    private static final String CURLY_SUFFIX = "}";
    private static final String VAR_DELIM = ",";
    private static final String DEFAULT_DELIM = ":";

    /**
     * Look in the supplied value for variables and replace them with values from the supplied lookup function.
     * <p>
     * Variables may appear anywhere within a string value, and multiple variables can be used within the same value. Variables
     * take the form:
     *
     * <pre>
     *    variable := '${' variableNames [ ':' defaultValue ] '}'
     *    variableNames := variableName [ ',' variableNames ]
     *    variableName := // any characters except ',' and ':' and '}'
     *    defaultValue := // any characters except '}'
     * </pre>
     *
     * Note that <i>variableName</i> is the name used to look up a the property.
     * </p>
     * Notice that the syntax supports multiple <i>variables</i>. The logic will process the <i>variables</i> from let to right,
     * until an existing property is found. And at that point, it will stop and will not attempt to find values for the other
     * <i>variables</i>.
     * <p>
     *
     * @param value the content in which the variables are to be found and replaced; may not be null
     * @param replacementsByVariableName the function used to find the replacements for variable names; may not be null
     * @return the function that will replace variables in supplied strings; never null
     */
    public static String replaceVariables(String value, Function<String, String> replacementsByVariableName) {
        if (value == null || value.trim().length() == 0) {
            return value;
        }

        StringBuilder sb = new StringBuilder(value);

        // Get the index of the first constant, if any
        int startName = sb.indexOf(CURLY_PREFIX);
        if (startName == -1) {
            return value;
        }

        // process as many different variable groupings that are defined, where one group will resolve to one property
        // substitution
        while (startName != -1) {
            String defaultValue = null;
            int endName = sb.indexOf(CURLY_SUFFIX, startName);
            if (endName == -1) {
                // if no suffix can be found, then this variable was probably defined incorrectly
                // but return what there is at this point
                return sb.toString();
            }

            String varString = sb.substring(startName + 2, endName);
            if (varString.indexOf(DEFAULT_DELIM) > -1) {
                List<String> defaults = split(varString, DEFAULT_DELIM);

                // get the property(s) variables that are defined left of the default delimiter.
                varString = defaults.get(0);

                // if the default is defined, then capture in case none of the other properties are found
                if (defaults.size() == 2) {
                    defaultValue = defaults.get(1);
                }
            }

            String constValue = null;
            // split the property(s) based VAR_DELIM, when multiple property options are defined
            List<String> vars = split(varString, VAR_DELIM);
            for (final String var : vars) {
                constValue = replacementsByVariableName.apply(var);

                // the first found property is the value to be substituted
                if (constValue != null) {
                    break;
                }
            }

            // if no property is found to substitute, then use the default value, if defined
            if (constValue == null && defaultValue != null) {
                constValue = defaultValue;
            }

            if (constValue != null) {
                sb = sb.replace(startName, endName + 1, constValue);
                // Checking for another constants
                startName = sb.indexOf(CURLY_PREFIX);

            }
            else {
                // continue to try to substitute for other properties so that all defined variables
                // are tried to be substituted for
                startName = sb.indexOf(CURLY_PREFIX, endName);
            }
        }
        return sb.toString();

    }

    /**
     * Split a string into pieces based on delimiters. Similar to the Perl function of the same name. The delimiters are not
     * included in the returned strings.
     *
     * @param str Full string
     * @param splitter Characters to split on
     * @return List of String pieces from full string
     */
    private static List<String> split(String str,
                                      String splitter) {
        StringTokenizer tokens = new StringTokenizer(str, splitter);
        ArrayList<String> l = new ArrayList<>(tokens.countTokens());
        while (tokens.hasMoreTokens()) {
            l.add(tokens.nextToken());
        }
        return l;
    }

    /**
     * Determine if the supplied string is a valid {@link UUID}.
     * @param str the string to evaluate
     * @return {@code true} if the string is a valid representation of a UUID, or {@code false} otherwise
     */
    public static boolean isUuid(String str) {
        if (str == null) {
            return false;
        }
        try {
            UUID.fromString(str);
            return true;
        }
        catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Check if the string is empty or null.
     *
     * @param str the string to check
     * @return {@code true} if the string is empty or null
     */
    public static boolean isNullOrEmpty(String str) {
        return str == null || str.isEmpty();
    }

    /**
     * Check if the string is blank (i.e. it's blank or only contains whitespace characters) or null.
     *
     * @param str the string to check
     * @return {@code true} if the string is blank or null
     */
    public static boolean isNullOrBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    /**
     * Checks if the value is empty or null, returning the default value if true, otherwise the specified value.
     *
     * @param value the string to check
     * @param defaultValue the default value to return
     * @return value if not empty or null; default value otherwise
     */
    public static String defaultIfEmpty(String value, String defaultValue) {
        return isNullOrEmpty(value) ? defaultValue : value;
    }

    /**
     * Checks if the value is blank (i.e. it's blank or only contains whitespace characters) or null, returning
     * the default value if true, otherwise returning the specified value.
     *
     * @param value the string to check
     * @param defaultValue the default value to return
     * @return value if not blank or null; default value otherwise
     */
    public static String defaultIfBlank(String value, String defaultValue) {
        return isNullOrBlank(value) ? defaultValue : value;
    }

    /**
     * Check if the string contains only digits.
     *
     * @param str the string to check
     * @return {@code true} if only contains digits
     */
    public static boolean isNumeric(String str) {
        if (isNullOrEmpty(str)) {
            return false;
        }
        final int sz = str.length();
        for (int i = 0; i < sz; i++) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Unquotes the given identifier part (e.g. an unqualified table name), if the
     * first character is one of the supported quoting characters (single quote,
     * double quote and back-tick) and the last character equals to the first
     * character. Otherwise, the original string will be returned.
     */
    public static String unquoteIdentifierPart(String identifierPart) {
        if (identifierPart == null || identifierPart.length() < 2) {
            return identifierPart;
        }

        Character quotingChar = deriveQuotingChar(identifierPart);
        if (quotingChar != null) {
            identifierPart = identifierPart.substring(1, identifierPart.length() - 1);
            identifierPart = identifierPart.replace(quotingChar.toString() + quotingChar.toString(), quotingChar.toString());
        }

        return identifierPart;
    }

    /**
     * Restores a byte array that is encoded as a hex string.
     */
    public static byte[] hexStringToByteArray(String hexString) {
        if (hexString == null) {
            return null;
        }

        int length = hexString.length();

        byte[] bytes = new byte[length / 2];
        for (int i = 0; i < length; i += 2) {
            bytes[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                    + Character.digit(hexString.charAt(i + 1), 16));
        }

        return bytes;
    }

    /**
     * Whether the given string begins with the given prefix, ignoring casing.
     * Copied from https://github.com/spring-projects/spring-framework/blob/master/spring-core/src/main/java/org/springframework/util/StringUtils.java.
     */
    public static boolean startsWithIgnoreCase(String str, String prefix) {
        return (str != null && prefix != null && str.length() >= prefix.length() &&
                str.regionMatches(true, 0, prefix, 0, prefix.length()));
    }

    /**
     * Returns the first {@code length} characters of the given string.
     *
     * @param str    The string to get the begin from
     * @param length The length of the string to return
     * @return {@code null}, if the given string is {@code null}, the first first
     *         {@code length} characters of that string otherwise. If the string is
     *         shorter than the given number of characters, the string itself will
     *         be returned.
     */
    public static String getBegin(String str, int length) {
        if (str == null) {
            return null;
        }
        else if (str.length() < length) {
            return str;
        }
        else {
            return str.substring(0, length);
        }
    }

    private static Character deriveQuotingChar(String identifierPart) {
        char first = identifierPart.charAt(0);
        char last = identifierPart.charAt(identifierPart.length() - 1);

        if (first == last && (first == '"' || first == '\'' || first == '`')) {
            return first;
        }

        return null;
    }

    /**
     * Masks sensitive data in given string
     *
     * @param original original string containing possibly sensitive data
     * @param mask replacement string
     * @param sensitives sensitive data to be masked
     * @return original string with sensitive data masked
     */
    public static String mask(String original, String mask, String... sensitives) {
        return Arrays.stream(sensitives)
                .filter(Objects::nonNull)
                .reduce(original, (masked, sensitive) -> masked.replace(sensitive, "***"));
    }

    private Strings() {
    }

    /**
     * A tokenization class used to split a comma-separated list of regular expressions.
     * If a comma is part of expression then it can be prepended with <code>'\'</code> so
     * it will not act as a separator.
     */
    public static class RegExSplitter implements Tokenizer {

        public static String[] split(String identifier) {
            TokenStream stream = new TokenStream(identifier, new RegExSplitter(), true);
            stream.start();

            List<String> parts = new ArrayList<>();

            while (stream.hasNext()) {
                final String part = stream.consume();
                if (part.length() == 0) {
                    continue;
                }
                parts.add(part.trim().replace("\\,", ","));
            }

            return parts.toArray(new String[parts.size()]);
        }

        @Override
        public void tokenize(CharacterStream input, Tokens tokens) throws ParsingException {
            int tokenStart = 0;
            while (input.hasNext()) {
                char c = input.next();
                // Escape sequence
                if (c == '\\') {
                    if (!input.hasNext()) {
                        throw new ParsingException(input.position(input.index()), "Unterminated escape sequence at the end of the string");
                    }
                    input.next();
                }
                else if (c == ',') {
                    tokens.addToken(input.position(tokenStart), tokenStart, input.index());
                    tokenStart = input.index() + 1;
                }
            }
            tokens.addToken(input.position(tokenStart), tokenStart, input.index() + 1);
        }
    }

    /**
     * Converts a string with separators (e.g., dots, underscores) into camelCase format using Stream API.
     *
     * @param input the input string containing separators such as dots or underscores
     * @return the converted string in camelCase format, or an empty string if the input is null or empty
     */
    public static String convertDotAndUnderscoreStringToCamelCase(String input) {
        if (input == null || input.isEmpty()) {
            return "";
        }

        String[] words = input.split("[._]+");
        if (words.length == 0) {
            return ""; // Handle edge case where input contains only separators
        }

        return java.util.stream.IntStream.range(0, words.length)
                .filter(i -> !words[i].isEmpty()) // Skip empty segments caused by consecutive separators
                .mapToObj(i -> i == 0
                        ? words[i].toLowerCase() // Ensure the first word starts with lowercase
                        : capitalizeFirstLetter(words[i])) // Capitalize the first letter of subsequent words
                .collect(java.util.stream.Collectors.joining());
    }

    /**
     * Capitalizes the first letter of a word and converts the rest to lowercase.
     *
     * @param word the word to capitalize
     * @return the word with the first letter capitalized
     */
    private static String capitalizeFirstLetter(String word) {
        if (word.isEmpty()) {
            return "";
        }
        return Character.toUpperCase(word.charAt(0)) + word.substring(1).toLowerCase();
    }

    /**
     * Converts a given string to snake_case format.
     * <p>
     * This method handles several common cases of transformations:
     * <ul>
     *     <li>Converts camelCase to snake_case (e.g., "camelCaseName" -> "camel_case_name").</li>
     *     <li>Inserts underscores between letters and numbers (e.g., "name123" -> "name_123").</li>
     *     <li>Inserts underscores between numbers and letters (e.g., "123name" -> "123_name").</li>
     *     <li>Replaces dots (.) with underscores (_).</li>
     * </ul>
     * <p>
     * The resulting string is converted to lowercase.
     * </p>
     *
     * <h3>Examples:</h3>
     * <pre>
     * {@code
     * toSnakeCase("camelCaseName");       // Returns "camel_case_name"
     * toSnakeCase("NameWith123Numbers"); // Returns "name_with_123_numbers"
     * toSnakeCase("123NumbersExample");  // Returns "123_numbers_example"
     * toSnakeCase("dotted.name");        // Returns "dotted_name"
     * toSnakeCase(null);                 // Returns null
     * }
     * </pre>
     *
     * @param name the original string to be converted; may be {@code null}
     * @return the string transformed to snake_case, or {@code null} if the input was {@code null}
     */
    public static String toSnakeCase(String name) {
        if (name == null) {
            return null;
        }
        // Combine all rules into a single regular expression
        return name.replaceAll("([a-z])([A-Z])|([a-zA-Z])([0-9])|([0-9])([a-zA-Z])|\\.", "$1$3$5_$2$4$6")
                .toLowerCase(Locale.ROOT);
    }

}
