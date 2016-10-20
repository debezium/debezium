/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.topic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.debezium.annotation.NotThreadSafe;

/**
 * A class used to build a {@link TopicSelector} from one or more {@link TopicNamingStrategy} definitions.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public class TopicSelectorBuilder {

    /**
     * The aliases for the built-in {@link TopicNamingStrategy} implementation classes. This is an immutable map.
     */
    public static final Map<String, Class<? extends TopicNamingStrategy>> ALIASES_TO_CLASSNAME;

    static {
        Map<String, Class<? extends TopicNamingStrategy>> aliases = new HashMap<>();
        aliases.put(ByDatabaseTopicNamingStrategy.ALIAS, ByDatabaseTopicNamingStrategy.class);
        aliases.put(ByTableTopicNamingStrategy.ALIAS, ByTableTopicNamingStrategy.class);
        ALIASES_TO_CLASSNAME = Collections.unmodifiableMap(aliases);
    }

    private ClassLoader classLoader;
    private List<TopicNamingStrategy> strategies = new ArrayList<>();
    private TopicNamingStrategy fallback;

    public TopicSelectorBuilder(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    /**
     * Add to this builder the {@link TopicNamingStrategy} instances defined by the comma-separated definitions.
     * 
     * @param commaSeparatedDefinitions the comma-separated strategy definitions, which may each be the name of a
     *            {@link TopicNamingStrategy} implementation class, an {@link #ALIASES_TO_CLASSNAME alias} for one of the
     *            built-in {@link TopicNamingStrategy} classes, or a regular expression and replacement pattern; may not be null
     *            or empty
     * @return this builder so methods can be chained; never null
     * @throws IllegalArgumentException if any of the {@link TopicNamingStrategy} instances cannot be created from the
     *             definition
     */
    public TopicSelectorBuilder addStrategies(String commaSeparatedDefinitions) {
        String[] parts = commaSeparatedDefinitions.split(",");
        for (String classOrPattern : parts) {
            classOrPattern = classOrPattern.trim();
            addStrategy(classOrPattern);
        }
        return this;
    }

    /**
     * Add to this builder the specified {@link TopicNamingStrategy} instance.
     * 
     * @param strategy the next topic naming strategy; may not be null
     * @return this builder so methods can be chained; never null
     */
    public TopicSelectorBuilder addStrategy(TopicNamingStrategy strategy) {
        assert strategy != null;
        strategies.add(strategy);
        return this;
    }

    /**
     * Use the given {@link TopicNamingStrategy} instance as a fallback, if all other strategies fail to produce a topic name.
     * The supplied strategy should never return a null topic name.
     * 
     * @param strategy the fallback topic naming strategy; may not be null
     * @return this builder so methods can be chained; never null
     */
    public TopicSelectorBuilder addFallback(TopicNamingStrategy strategy) {
        assert strategy != null;
        fallback = strategy;
        return this;
    }

    /**
     * Add to this builder a {@link TopicNamingStrategy} instance given the class name, alias, or regular expression and
     * replacement pattern.
     * 
     * @param classNameOrRegexPattern the name of a {@link TopicNamingStrategy} implementation class, an
     *            {@link #ALIASES_TO_CLASSNAME alias} for one of the built-in {@link TopicNamingStrategy} classes, or a
     *            regular expression and replacement pattern; may not be null or empty
     * @return this builder so methods can be chained; never null
     * @throws IllegalArgumentException if the {@link TopicNamingStrategy} cannot be instantiated
     */
    public TopicSelectorBuilder addStrategy(String classNameOrRegexPattern) {
        // See if the value is an alias or class name ...
        Class<? extends TopicNamingStrategy> strategyClass = ALIASES_TO_CLASSNAME.get(classNameOrRegexPattern.toLowerCase());
        if (strategyClass != null) {
            return addStrategy(strategyClass);
        }
        
        // See if the value is the name of a custom class ...
        try {
            Class<?> clazz = classLoader.loadClass(classNameOrRegexPattern);
            strategyClass = clazz.asSubclass(TopicNamingStrategy.class);
            return addStrategy(strategyClass);
        } catch (ClassNotFoundException e) {
            // Might not be a class name
        } catch (ClassCastException e) {
            // Valid class, but does not implement interface ...
            throw new IllegalArgumentException(
                    "The " + strategyClass.getName() + " class does not implement " + TopicNamingStrategy.class.getName(), e);
        }

        // Try treating this as a table pattern ...
        try {
            TopicNamingStrategy strategy = new ByTablePatternsTopicNamingStrategy(classNameOrRegexPattern);
            addStrategy(strategy);
        } catch (Throwable e) {
            throw new IllegalArgumentException("Unable to create a topic naming strategy using '" + classNameOrRegexPattern
                    + "'. If this was a class name, the class could not be found. Otherwise, the patterns could not be recognized: "
                    + e.getMessage(), e);
        }
        return this;
    }

    /**
     * Add to this builder the {@link TopicNamingStrategy} with the given class.
     * 
     * @param strategyClass the class that is to be instantiated; may not be null
     * @return this builder so methods can be chained; never null
     */
    public TopicSelectorBuilder addStrategy(Class<? extends TopicNamingStrategy> strategyClass) {
        try {
            addStrategy(strategyClass.newInstance());
        } catch (InstantiationException | IllegalAccessException e) {
            // Problems instantiating or accessing the class ...
            throw new IllegalArgumentException("Unable to instantiate or access the " + strategyClass.getName() + " class", e);
        }
        return this;
    }

    /**
     * Create a {@link TopicSelector} that uses the given prefix and the {@link TopicNamingStrategy}s already added to this
     * builder.
     * 
     * @param prefix the prefix for the topic names; may not be null or empty
     * @return the topic selector; never null
     */
    public TopicSelector buildUsingPrefix(String prefix) {
        List<TopicNamingStrategy> strategies = new ArrayList<>(this.strategies);
        if (fallback != null) strategies.add(fallback);
        return new TopicSelector() {
            @Override
            public String getPrimaryTopic() {
                return prefix;
            }

            @Override
            public String getTopic(String databaseName, String tableName) {
                String name = null;
                for (TopicNamingStrategy strategy : strategies) {
                    name = strategy.getTopic(prefix, databaseName, tableName);
                    if (name != null) break;
                }
                assert name != null;
                return name;
            }
        };
    }
}
