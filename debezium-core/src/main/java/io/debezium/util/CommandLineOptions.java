/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import io.debezium.annotation.Immutable;

/**
 * Utility for parsing and accessing the command line options and parameters.
 * 
 * @author Randall Hauch
 */
@Immutable
public class CommandLineOptions {

    /**
     * Parse the array of arguments passed to a Java {@code main} method and create a {@link CommandLineOptions} instance.
     * 
     * @param args the {@code main} method's parameters; may not be null
     * @return the representation of the command line options and parameters; never null
     */
    public static CommandLineOptions parse(String[] args) {
        Map<String, String> options = new HashMap<>();
        List<String> params = new ArrayList<>();
        List<String> optionNames = new ArrayList<>();
        String optionName = null;
        for (String value : args) {
            value = value.trim();
            if (value.startsWith("-")) {
                optionName = value;
                options.put(optionName, "true");
                optionNames.add(optionName);
            } else {
                if (optionName != null)
                    options.put(optionName, value);
                else
                    params.add(value);
            }
        }
        return new CommandLineOptions(options, params, optionNames);
    }

    private final Map<String, String> options;
    private final List<String> params;
    private final List<String> orderedOptionNames;

    private CommandLineOptions(Map<String, String> options, List<String> params, List<String> orderedOptionNames) {
        this.options = options;
        this.params = params;
        this.orderedOptionNames = orderedOptionNames;
    }

    /**
     * Determine if the option with the given name was used on the command line.
     * <p>
     * The supplied option name is trimmed before matching against the command line arguments. Note that any special characters
     * such as prefixes (e.g., '{@code -}' or '{@code --}') must be included in the name.
     * 
     * @param name the name for the option (e.g., "-v" or "--verbose")
     * @return true if the exact option was used, or false otherwise or if the supplied option name is null
     * @see #hasOption(String, String)
     */
    public boolean hasOption(String name) {
        return hasOption(name, null);
    }

    /**
     * Determine if the option with one of the given names was used on the command line.
     * <p>
     * The supplied option name and alternative names are trimmed before matching against the command line arguments. Note that
     * any special characters such as prefixes (e.g., '{@code -}' or '{@code --}') must be included in the name.
     * 
     * @param name the name for the option (e.g., "-v" or "--verbose")
     * @param alternativeName an alternative name for the option; may be null
     * @return true if the exact option was used, or false otherwise or if both the supplied option name and alternative name are
     *         null
     * @see #hasOption(String)
     */
    public boolean hasOption(String name, String alternativeName) {
        return getOption(name, alternativeName, null) != null;
    }

    /**
     * Obtain the value associated with the named option.
     * <p>
     * The supplied option name is trimmed before matching against the command line arguments. Note that any special characters
     * such as prefixes (e.g., '{@code -}' or '{@code --}') must be included in the name.
     * 
     * @param name the name for the option (e.g., "-v" or "--verbose")
     * @return the value associated with the option, or null if no option was found or the name is null
     * @see #getOption(String, String)
     * @see #getOption(String, String, String)
     */
    public String getOption(String name) {
        return getOption(name, null);
    }

    /**
     * Obtain the value associated with the named option, using the supplied default value if none is found.
     * <p>
     * The supplied option name is trimmed before matching against the command line arguments. Note that any special characters
     * such as prefixes (e.g., '{@code -}' or '{@code --}') must be included in the name.
     * 
     * @param name the name for the option (e.g., "-v" or "--verbose")
     * @param defaultValue the value that should be returned no named option was found
     * @return the value associated with the option, or the default value if none was found or the name is null
     * @see #getOption(String)
     * @see #getOption(String, String, String)
     */
    public String getOption(String name, String defaultValue) {
        return getOption(name, null, defaultValue);
    }

    /**
     * Obtain the value associated with the option given the name and alternative name of the option, using the supplied default
     * value if none is found.
     * <p>
     * The supplied option name and alternative names are trimmed before matching against the command line arguments. Note that
     * any special characters such as prefixes (e.g., '{@code -}' or '{@code --}') must be included in the name.
     * 
     * @param name the name for the option (e.g., "-v" or "--verbose")
     * @param alternativeName an alternative name for the option; may be null
     * @param defaultValue the value that should be returned no named option was found
     * @return the value associated with the option, or the default value if none was found or if both the name and alternative
     *         name are null
     * @see #getOption(String)
     * @see #getOption(String, String)
     */
    public String getOption(String name, String alternativeName, String defaultValue) {
        recordOptionUsed(name, alternativeName);
        String result = options.get(name.trim());
        if (result == null && alternativeName != null) result = options.get(alternativeName.trim());
        return result != null ? result : defaultValue;
    }

    protected void recordOptionUsed(String name, String alternativeName) {
        orderedOptionNames.remove(name.trim());
        if (alternativeName != null) orderedOptionNames.remove(alternativeName.trim());
    }

    /**
     * Obtain the long value associated with the named option, using the supplied default value if none is found or if the value
     * cannot be parsed as a long value.
     * <p>
     * The supplied option name is trimmed before matching against the command line arguments. Note that any special characters
     * such as prefixes (e.g., '{@code -}' or '{@code --}') must be included in the name.
     * 
     * @param name the name for the option (e.g., "-v" or "--verbose")
     * @param defaultValue the value that should be returned no named option was found
     * @return the value associated with the option, or the default value if none was found or the name is null
     * @see #getOption(String, String, long)
     */
    public long getOption(String name, long defaultValue) {
        return getOption(name, null, defaultValue);
    }

    /**
     * Obtain the long value associated with the option given the name and alternative name of the option, using the supplied
     * default value if none is found or if the value cannot be parsed as a long value.
     * <p>
     * The supplied option name and alternative names are trimmed before matching against the command line arguments. Note that
     * any special characters such as prefixes (e.g., '{@code -}' or '{@code --}') must be included in the name.
     * 
     * @param name the name for the option (e.g., "-v" or "--verbose")
     * @param alternativeName an alternative name for the option; may be null
     * @param defaultValue the value that should be returned no named option was found
     * @return the value associated with the option, or the default value if none was found or if both the name and alternative
     *         name are null
     * @see #getOption(String, long)
     */
    public long getOption(String name, String alternativeName, long defaultValue) {
        return Strings.asLong(getOption(name, alternativeName, null), defaultValue);
    }

    /**
     * Obtain the integer value associated with the named option, using the supplied default value if none is found or if the
     * value
     * cannot be parsed as an integer value.
     * <p>
     * The supplied option name is trimmed before matching against the command line arguments. Note that any special characters
     * such as prefixes (e.g., '{@code -}' or '{@code --}') must be included in the name.
     * 
     * @param name the name for the option (e.g., "-v" or "--verbose")
     * @param defaultValue the value that should be returned no named option was found
     * @return the value associated with the option, or the default value if none was found or the name is null
     * @see #getOption(String, String, int)
     */
    public int getOption(String name, int defaultValue) {
        return getOption(name, null, defaultValue);
    }

    /**
     * Obtain the integer value associated with the option given the name and alternative name of the option, using the supplied
     * default value if none is found or if the value cannot be parsed as an integer value.
     * <p>
     * The supplied option name and alternative names are trimmed before matching against the command line arguments. Note that
     * any special characters such as prefixes (e.g., '{@code -}' or '{@code --}') must be included in the name.
     * 
     * @param name the name for the option (e.g., "-v" or "--verbose")
     * @param alternativeName an alternative name for the option; may be null
     * @param defaultValue the value that should be returned no named option was found
     * @return the value associated with the option, or the default value if none was found or if both the name and alternative
     *         name are null
     * @see #getOption(String, int)
     */
    public int getOption(String name, String alternativeName, int defaultValue) {
        return Strings.asInt(getOption(name, alternativeName, null), defaultValue);
    }

    /**
     * Obtain the boolean value associated with the named option, using the supplied default value if none is found or if the
     * value
     * cannot be parsed as a boolean value.
     * <p>
     * The supplied option name is trimmed before matching against the command line arguments. Note that any special characters
     * such as prefixes (e.g., '{@code -}' or '{@code --}') must be included in the name.
     * 
     * @param name the name for the option (e.g., "-v" or "--verbose")
     * @param defaultValue the value that should be returned no named option was found
     * @return the value associated with the option, or the default value if none was found or the name is null
     * @see #getOption(String, String, boolean)
     */
    public boolean getOption(String name, boolean defaultValue) {
        return getOption(name, null, defaultValue);
    }

    /**
     * Obtain the boolean value associated with the option given the name and alternative name of the option, using the supplied
     * default value if none is found or if the value cannot be parsed as a boolean value.
     * <p>
     * The supplied option name and alternative names are trimmed before matching against the command line arguments. Note that
     * any special characters such as prefixes (e.g., '{@code -}' or '{@code --}') must be included in the name.
     * 
     * @param name the name for the option (e.g., "-v" or "--verbose")
     * @param alternativeName an alternative name for the option; may be null
     * @param defaultValue the value that should be returned no named option was found
     * @return the value associated with the option, or the default value if none was found or if both the name and alternative
     *         name are null
     * @see #getOption(String, boolean)
     */
    public boolean getOption(String name, String alternativeName, boolean defaultValue) {
        return Strings.asBoolean(getOption(name, alternativeName, null), defaultValue);
    }

    /**
     * Obtain the parameter at the given index. Parameters are those arguments that are not preceded by an option name.
     * 
     * @param index the index of the parameter
     * @return the parameter value, or null if there was no parameter at the given index
     */
    public String getParameter(int index) {
        return index < 0 || index >= params.size() ? null : params.get(index);
    }

    /**
     * Determine whether the specified value matches one of the parameters.
     * 
     * @param value the parameter value to match
     * @return true if one of the parameter matches the value, or false otherwise
     */
    public boolean hasParameter(String value) {
        return value == null || params.isEmpty() ? false : params.contains(value);
    }

    /**
     * Determine whether there were any unknown option names after all possible options have been checked via one of the
     * {@code getOption(String,...)} methods.
     * 
     * @return true if there was at least one option (e.g., beginning with '{@code -}') that was not checked, or false
     * if there were no unknown options on the command line
     * @see #getFirstUnknownOptionName()
     * @see #hasUnknowns()
     */
    public boolean hasUnknowns() {
        return !orderedOptionNames.isEmpty();
    }

    /**
     * Get the list of unknown option names after all possible options have been checked via one of the
     * {@code getOption(String,...)} methods.
     * 
     * @return the list of options (e.g., beginning with '{@code -}') that were not checked; never null but possible empty
     * @see #getFirstUnknownOptionName()
     * @see #hasUnknowns()
     */
    public List<String> getUnknownOptionNames() {
        return Collections.unmodifiableList(orderedOptionNames);
    }

    /**
     * If there were {@link #hasUnknowns() unknown options}, return the first one that appeared on the command line.
     * @return the first unknown option, or null if there were no {@link #hasUnknowns() unknown options}
     * @see #getUnknownOptionNames()
     * @see #getFirstUnknownOptionName()
     */
    public String getFirstUnknownOptionName() {
        return orderedOptionNames.isEmpty() ? null : orderedOptionNames.get(0);
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(" ");
        options.forEach((opt, val) -> {
            joiner.add("-" + opt).add(val);
        });
        params.forEach(joiner::add);
        return joiner.toString();
    }

}
