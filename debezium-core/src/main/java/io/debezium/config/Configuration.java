/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.util.Collect;
import io.debezium.util.IoUtil;

/**
 * An immutable representation of a Debezium configuration. A {@link Configuration} instance can be obtained
 * {@link #from(Properties) from Properties} or loaded from a {@link #load(File) file}, {@link #load(InputStream) stream},
 * {@link #load(Reader) reader}, {@link #load(URL) URL}, or {@link #load(String, ClassLoader) classpath resource}. They can
 * also be built by first {@link #create() creating a builder} and then using that builder to populate and
 * {@link Builder#build() return} the immutable Configuration instance.
 * <p>
 * A Configuration object is basically a decorator around a {@link Properties} object. It has methods to get and convert
 * individual property values to numeric, boolean and String types, optionally using a default value if the given property value
 * does not exist. However, it is {@link Immutable immutable}, so it does not have any methods to set property values, allowing
 * it to be passed around and reused without concern that other components might change the underlying property values.
 * 
 * @author Randall Hauch
 */
@Immutable
public interface Configuration {

    /**
     * The basic interface for configuration builders.
     * 
     * @param <C> the type of configuration
     * @param <B> the type of builder
     */
    public static interface ConfigBuilder<C extends Configuration, B extends ConfigBuilder<C, B>> {
        /**
         * Associate the given value with the specified key.
         * 
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        B with(String key, String value);

        /**
         * Associate the given value with the specified key.
         * 
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(String key, Object value) {
            return with(key, value != null ? value.toString() : null);
        }

        /**
         * Associate the given value with the specified key.
         * 
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(String key, int value) {
            return with(key, Integer.toString(value));
        }

        /**
         * Associate the given value with the specified key.
         * 
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(String key, float value) {
            return with(key, Float.toString(value));
        }

        /**
         * Associate the given value with the specified key.
         * 
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(String key, double value) {
            return with(key, Double.toString(value));
        }

        /**
         * Associate the given value with the specified key.
         * 
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(String key, long value) {
            return with(key, Long.toString(value));
        }

        /**
         * Associate the given value with the specified key.
         * 
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(String key, boolean value) {
            return with(key, Boolean.toString(value));
        }

        /**
         * If there is no field with the specified key, then associate the given value with the specified key.
         * 
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        B withDefault(String key, String value);

        /**
         * If there is no field with the specified key, then associate the given value with the specified key.
         * 
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B withDefault(String key, Object value) {
            return withDefault(key, value != null ? value.toString() : null);
        }

        /**
         * If there is no field with the specified key, then associate the given value with the specified key.
         * 
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B withDefault(String key, int value) {
            return withDefault(key, Integer.toString(value));
        }

        /**
         * If there is no field with the specified key, then associate the given value with the specified key.
         * 
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B withDefault(String key, float value) {
            return withDefault(key, Float.toString(value));
        }

        /**
         * If there is no field with the specified key, then associate the given value with the specified key.
         * 
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B withDefault(String key, double value) {
            return withDefault(key, Double.toString(value));
        }

        /**
         * If there is no field with the specified key, then associate the given value with the specified key.
         * 
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B withDefault(String key, long value) {
            return withDefault(key, Long.toString(value));
        }

        /**
         * If there is no field with the specified key, then associate the given value with the specified key.
         * 
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B withDefault(String key, boolean value) {
            return withDefault(key, Boolean.toString(value));
        }

        /**
         * Associate the given value with the key of the specified field.
         * 
         * @param field the predefined field for the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(Field field, String value) {
            return with(field.name(), value);
        }

        /**
         * Associate the given value with the key of the specified field.
         * 
         * @param field the predefined field for the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(Field field, int value) {
            return with(field.name(), value);
        }

        /**
         * Associate the given value with the key of the specified field.
         * 
         * @param field the predefined field for the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(Field field, float value) {
            return with(field.name(), value);
        }

        /**
         * Associate the given value with the key of the specified field.
         * 
         * @param field the predefined field for the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(Field field, double value) {
            return with(field.name(), value);
        }

        /**
         * Associate the given value with the key of the specified field.
         * 
         * @param field the predefined field for the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(Field field, long value) {
            return with(field.name(), value);
        }

        /**
         * Associate the given value with the key of the specified field.
         * 
         * @param field the predefined field for the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(Field field, boolean value) {
            return with(field.name(), value);
        }

        /**
         * If the field does not have a value, then associate the given value with the key of the specified field.
         * 
         * @param field the predefined field for the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B withDefault(Field field, String value) {
            return withDefault(field.name(), value);
        }

        /**
         * If the field does not have a value, then associate the given value with the key of the specified field.
         * 
         * @param field the predefined field for the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B withDefault(Field field, int value) {
            return withDefault(field.name(), value);
        }

        /**
         * If the field does not have a value, then associate the given value with the key of the specified field.
         * 
         * @param field the predefined field for the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B withDefault(Field field, float value) {
            return withDefault(field.name(), value);
        }

        /**
         * If the field does not have a value, then associate the given value with the key of the specified field.
         * 
         * @param field the predefined field for the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B withDefault(Field field, double value) {
            return withDefault(field.name(), value);
        }

        /**
         * If the field does not have a value, then associate the given value with the key of the specified field.
         * 
         * @param field the predefined field for the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B withDefault(Field field, long value) {
            return withDefault(field.name(), value);
        }

        /**
         * If the field does not have a value, then associate the given value with the key of the specified field.
         * 
         * @param field the predefined field for the key
         * @param value the default value
         * @return this builder object so methods can be chained together; never null
         */
        default B withDefault(Field field, boolean value) {
            return withDefault(field.name(), value);
        }

        /**
         * Apply the function to this builder.
         * 
         * @param function the predefined field for the key
         * @return this builder object so methods can be chained together; never null
         */
        B apply(Consumer<B> function);

        /**
         * Build and return the immutable configuration.
         * 
         * @return the immutable configuration; never null
         */
        C build();
    }

    /**
     * A builder of Configuration objects.
     */
    public static class Builder implements ConfigBuilder<Configuration, Builder> {
        private final Properties props = new Properties();

        protected Builder() {
        }

        protected Builder(Properties props) {
            this.props.putAll(props);
        }

        @Override
        public Builder with(String key, String value) {
            props.setProperty(key, value);
            return this;
        }
        
        @Override
        public Builder withDefault(String key, String value) {
            if ( !props.containsKey(key)) {
                props.setProperty(key, value);
            }
            return this;
        }
        
        @Override
        public Builder apply(Consumer<Builder> function) {
            function.accept(this);
            return this;
        }

        @Override
        public Configuration build() {
            return Configuration.from(props);
        }
    }

    /**
     * Create a new {@link Builder configuration builder}.
     * 
     * @return the configuration builder
     */
    public static Builder create() {
        return new Builder();
    }

    /**
     * Create a new {@link Builder configuration builder} that starts with a copy of the supplied configuration.
     * 
     * @param config the configuration to copy
     * @return the configuration builder
     */
    public static Builder copy(Configuration config) {
        return new Builder(config.asProperties());
    }
    
    /**
     * Create a Configuration object that is populated by system properties, per {@link #withSystemProperties(String)}.
     * 
     * @param prefix the required prefix for the system properties; may not be null but may be empty
     * @return the configuration
     */
    public static Configuration fromSystemProperties(String prefix) {
        return empty().withSystemProperties(prefix);
    }

    /**
     * Obtain an empty configuration.
     * 
     * @return an empty configuration; never null
     */
    public static Configuration empty() {
        return new Configuration() {
            @Override
            public Set<String> keys() {
                return Collections.emptySet();
            }

            @Override
            public String getString(String key) {
                return null;
            }
            
            @Override
            public String toString() {
                return "{}";
            }
        };
    }

    /**
     * Obtain a configuration instance by copying the supplied Properties object. The supplied {@link Properties} object is
     * copied so that the resulting Configuration cannot be modified.
     * 
     * @param properties the properties; may be null or empty
     * @return the configuration; never null
     */
    public static Configuration from(Properties properties) {
        Properties props = new Properties();
        if (properties != null) props.putAll(properties);
        return new Configuration() {
            @Override
            public String getString(String key) {
                return properties.getProperty(key);
            }

            @Override
            public Set<String> keys() {
                return properties.stringPropertyNames();
            }

            @Override
            public String toString() {
                return props.toString();
            }
        };
    }

    /**
     * Obtain a configuration instance by copying the supplied map of string keys and string values. The entries within the map
     * are copied so that the resulting Configuration cannot be modified.
     * 
     * @param properties the properties; may be null or empty
     * @return the configuration; never null
     */
    public static Configuration from(Map<String, String> properties) {
        Map<String, String> props = new HashMap<>();
        if (properties != null) props.putAll(properties);
        return new Configuration() {
            @Override
            public String getString(String key) {
                return properties.get(key);
            }

            @Override
            public Set<String> keys() {
                return properties.keySet();
            }

            @Override
            public String toString() {
                return props.toString();
            }
        };
    }

    /**
     * Obtain a configuration instance by loading the Properties from the supplied URL.
     * 
     * @param url the URL to the stream containing the configuration properties; may not be null
     * @return the configuration; never null
     * @throws IOException if there is an error reading the stream
     */
    public static Configuration load(URL url) throws IOException {
        try (InputStream stream = url.openStream()) {
            return load(stream);
        }
    }

    /**
     * Obtain a configuration instance by loading the Properties from the supplied file.
     * 
     * @param file the file containing the configuration properties; may not be null
     * @return the configuration; never null
     * @throws IOException if there is an error reading the stream
     */
    public static Configuration load(File file) throws IOException {
        try (InputStream stream = new FileInputStream(file)) {
            return load(stream);
        }
    }

    /**
     * Obtain a configuration instance by loading the Properties from the supplied stream.
     * 
     * @param stream the stream containing the properties; may not be null
     * @return the configuration; never null
     * @throws IOException if there is an error reading the stream
     */
    public static Configuration load(InputStream stream) throws IOException {
        try {
            Properties properties = new Properties();
            properties.load(stream);
            return from(properties);
        } finally {
            stream.close();
        }
    }

    /**
     * Obtain a configuration instance by loading the Properties from the supplied reader.
     * 
     * @param reader the reader containing the properties; may not be null
     * @return the configuration; never null
     * @throws IOException if there is an error reading the stream
     */
    public static Configuration load(Reader reader) throws IOException {
        try {
            Properties properties = new Properties();
            properties.load(reader);
            return from(properties);
        } finally {
            reader.close();
        }
    }

    /**
     * Obtain a configuration instance by loading the Properties from a file on the file system or classpath given by the supplied
     * path.
     * 
     * @param path the path to the file containing the configuration properties; may not be null
     * @param clazz the class whose classpath is to be used to find the file; may be null
     * @return the configuration; never null but possibly empty
     * @throws IOException if there is an error reading the stream
     */
    public static Configuration load(String path, Class<?> clazz) throws IOException {
        return load(path, clazz.getClassLoader());
    }

    /**
     * Obtain a configuration instance by loading the Properties from a file on the file system or classpath given by the supplied
     * path.
     * 
     * @param path the path to the file containing the configuration properties; may not be null
     * @param classLoader the class loader to use; may be null
     * @return the configuration; never null but possibly empty
     * @throws IOException if there is an error reading the stream
     */
    public static Configuration load(String path, ClassLoader classLoader) throws IOException {
        Logger logger = LoggerFactory.getLogger(Configuration.class);
        return load(path, classLoader, logger::debug);
    }

    /**
     * Obtain a configuration instance by loading the Properties from a file on the file system or classpath given by the supplied
     * path.
     * 
     * @param path the path to the file containing the configuration properties; may not be null
     * @param classLoader the class loader to use; may be null
     * @param logger the function that will be called with status updates; may be null
     * @return the configuration; never null but possibly empty
     * @throws IOException if there is an error reading the stream
     */
    public static Configuration load(String path, ClassLoader classLoader, Consumer<String> logger) throws IOException {
        try (InputStream stream = IoUtil.getResourceAsStream(path, classLoader, null, null, logger)) {
            Properties props = new Properties();
            if (stream != null) {
                props.load(stream);
            }
            return from(props);
        }
    }
    
    /**
     * Obtain an editor for a copy of this configuration.
     * @return a builder that is populated with this configuration's key-value pairs; never null
     */
    default Builder edit() {
        return copy(this);
    }
    
    /**
     * Determine whether this configuration contains a key-value pair with the given key and the value is non-null
     * 
     * @param key the key
     * @return true if the configuration contains the key, or false otherwise
     */
    default boolean hasKey(String key) {
        return getString(key) != null;
    }

    /**
     * Get the set of keys in this configuration.
     * 
     * @return the set of keys; never null but possibly empty
     */
    public Set<String> keys();

    /**
     * Get the string value associated with the given key.
     * 
     * @param key the key for the configuration property
     * @return the value, or null if the key is null or there is no such key-value pair in the configuration
     */
    public String getString(String key);

    /**
     * Get the string value associated with the given key, returning the default value if there is no such key-value pair.
     * 
     * @param key the key for the configuration property
     * @param defaultValue the value that should be returned by default if there is no such key-value pair in the configuration;
     *            may be null
     * @return the configuration value, or the {@code defaultValue} if there is no such key-value pair in the configuration
     */
    default String getString(String key, String defaultValue) {
        return getString(key, () -> defaultValue);
    }

    /**
     * Get the string value associated with the given key, returning the default value if there is no such key-value pair.
     * 
     * @param key the key for the configuration property
     * @param defaultValueSupplier the supplier of value that should be returned by default if there is no such key-value pair in
     *            the configuration; may be null and may return null
     * @return the configuration value, or the {@code defaultValue} if there is no such key-value pair in the configuration
     */
    default String getString(String key, Supplier<String> defaultValueSupplier) {
        String value = getString(key);
        return value != null ? value : (defaultValueSupplier != null ? defaultValueSupplier.get() : null);
    }

    /**
     * Get the string value associated with the given field, returning the field's default value if there is no such key-value
     * pair in this configuration.
     * 
     * @param field the field; may not be null
     * @return the configuration's value for the field, or the field's {@link Field#defaultValue() default value} if there is no
     *         such key-value pair in the configuration
     */
    default String getString(Field field) {
        return getString(field.name(), field.defaultValue());
    }

    /**
     * Get the string value associated with the given field, returning the field's default value if there is no such key-value
     * pair in this configuration.
     * 
     * @param field the field; may not be null
     * @param defaultValue the default value
     * @return the configuration's value for the field, or the field's {@link Field#defaultValue() default value} if there is no
     *         such key-value pair in the configuration
     */
    default String getString(Field field, String defaultValue) {
        return getString(field.name(), ()->field.defaultValue());
    }

    /**
     * Get the string value(s) associated with the given key, where the supplied regular expression is used to parse the single
     * string value into multiple values.
     * 
     * @param key the key for the configuration property
     * @param regex the delimiting regular expression
     * @return the list of string values; null only if there is no such key-value pair in the configuration
     * @see String#split(String)
     */
    default List<String> getStrings(String key, String regex) {
        String value = getString(key);
        if (value == null) return null;
        return Collect.arrayListOf(value.split(regex));
    }

    /**
     * Get the integer value associated with the given key.
     * 
     * @param key the key for the configuration property
     * @return the integer value, or null if the key is null, there is no such key-value pair in the configuration, or the value
     *         could not be parsed as an integer
     */
    default Integer getInteger(String key) {
        return getInteger(key, null);
    }

    /**
     * Get the long value associated with the given key.
     * 
     * @param key the key for the configuration property
     * @return the integer value, or null if the key is null, there is no such key-value pair in the configuration, or the value
     *         could not be parsed as an integer
     */
    default Long getLong(String key) {
        return getLong(key, null);
    }

    /**
     * Get the boolean value associated with the given key.
     * 
     * @param key the key for the configuration property
     * @return the boolean value, or null if the key is null, there is no such key-value pair in the configuration, or the value
     *         could not be parsed as a boolean value
     */
    default Boolean getBoolean(String key) {
        return getBoolean(key, null);
    }

    /**
     * Get the integer value associated with the given key, returning the default value if there is no such key-value pair or
     * if the value could not be {@link Integer#parseInt(String) parsed} as an integer.
     * 
     * @param key the key for the configuration property
     * @param defaultValue the default value
     * @return the integer value, or null if the key is null, there is no such key-value pair in the configuration, or the value
     *         could not be parsed as an integer
     */
    default int getInteger(String key, int defaultValue) {
        return getInteger(key, () -> defaultValue).intValue();
    }

    /**
     * Get the long value associated with the given key, returning the default value if there is no such key-value pair or
     * if the value could not be {@link Long#parseLong(String) parsed} as a long.
     * 
     * @param key the key for the configuration property
     * @param defaultValue the default value
     * @return the long value, or null if the key is null, there is no such key-value pair in the configuration, or the value
     *         could not be parsed as a long
     */
    default long getLong(String key, long defaultValue) {
        return getLong(key, () -> defaultValue).longValue();
    }

    /**
     * Get the boolean value associated with the given key, returning the default value if there is no such key-value pair or
     * if the value could not be {@link Boolean#parseBoolean(String) parsed} as a boolean value.
     * 
     * @param key the key for the configuration property
     * @param defaultValue the default value
     * @return the boolean value, or null if the key is null, there is no such key-value pair in the configuration, or the value
     *         could not be parsed as a boolean value
     */
    default boolean getBoolean(String key, boolean defaultValue) {
        return getBoolean(key, () -> defaultValue).booleanValue();
    }

    /**
     * Get the integer value associated with the given key, using the given supplier to obtain a default value if there is no such
     * key-value pair.
     * 
     * @param key the key for the configuration property
     * @param defaultValueSupplier the supplier for the default value; may be null
     * @return the integer value, or null if the key is null, there is no such key-value pair in the configuration, the
     *         {@code defaultValueSupplier} reference is null, or there is a key-value pair in the configuration but the value
     *         could not be parsed as an integer
     */
    default Integer getInteger(String key, IntSupplier defaultValueSupplier) {
        String value = getString(key);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
            }
        }
        return defaultValueSupplier != null ? defaultValueSupplier.getAsInt() : null;
    }

    /**
     * Get the long value associated with the given key, using the given supplier to obtain a default value if there is no such
     * key-value pair.
     * 
     * @param key the key for the configuration property
     * @param defaultValueSupplier the supplier for the default value; may be null
     * @return the long value, or null if the key is null, there is no such key-value pair in the configuration, the
     *         {@code defaultValueSupplier} reference is null, or there is a key-value pair in the configuration but the value
     *         could not be parsed as a long
     */
    default Long getLong(String key, LongSupplier defaultValueSupplier) {
        String value = getString(key);
        if (value != null) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
            }
        }
        return defaultValueSupplier != null ? defaultValueSupplier.getAsLong() : null;
    }

    /**
     * Get the boolean value associated with the given key, using the given supplier to obtain a default value if there is no such
     * key-value pair.
     * 
     * @param key the key for the configuration property
     * @param defaultValueSupplier the supplier for the default value; may be null
     * @return the boolean value, or null if the key is null, there is no such key-value pair in the configuration, the
     *         {@code defaultValueSupplier} reference is null, or there is a key-value pair in the configuration but the value
     *         could not be parsed as a boolean value
     */
    default Boolean getBoolean(String key, BooleanSupplier defaultValueSupplier) {
        String value = getString(key);
        if (value != null) {
            value = value.trim().toLowerCase();
            if (Boolean.valueOf(value)) return Boolean.TRUE;
            if (value.equals("false")) return false;
        }
        return defaultValueSupplier != null ? defaultValueSupplier.getAsBoolean() : null;
    }

    /**
     * Get the integer value associated with the given field, returning the field's default value if there is no such
     * key-value pair.
     * 
     * @param field the field
     * @return the integer value, or null if the key is null, there is no such key-value pair in the configuration and there is
     *         no default value in the field or the default value could not be parsed as a long, or there is a key-value pair in
     *         the configuration but the value could not be parsed as an integer value
     * @throws NumberFormatException if there is no name-value pair and the field has no default value
     */
    default int getInteger(Field field) {
        return getInteger(field.name(), ()->Integer.valueOf(field.defaultValue())).intValue();
    }

    /**
     * Get the long value associated with the given field, returning the field's default value if there is no such
     * key-value pair.
     * 
     * @param field the field
     * @return the integer value, or null if the key is null, there is no such key-value pair in the configuration and there is
     *         no default value in the field or the default value could not be parsed as a long, or there is a key-value pair in
     *         the configuration but the value could not be parsed as a long value
     * @throws NumberFormatException if there is no name-value pair and the field has no default value
     */
    default long getLong(Field field) {
        return getLong(field.name(), ()->Long.valueOf(field.defaultValue())).longValue();
    }

    /**
     * Get the boolean value associated with the given field when that field has a default value. If the configuration does
     * not have a name-value pair with the same name as the field, then the field's default value.
     * 
     * @param field the field
     * @return the boolean value, or null if the key is null, there is no such key-value pair in the configuration and there is
     *         no default value in the field or the default value could not be parsed as a long, or there is a key-value pair in
     *         the configuration but the value could not be parsed as a boolean value
     * @throws NumberFormatException if there is no name-value pair and the field has no default value
     */
    default boolean getBoolean(Field field) {
        return getBoolean(field.name(), ()->Boolean.valueOf(field.defaultValue())).booleanValue();
    }

    /**
     * Get the integer value associated with the given field, returning the field's default value if there is no such
     * key-value pair.
     * 
     * @param field the field
     * @param defaultValue the default value
     * @return the integer value, or null if the key is null, there is no such key-value pair in the configuration and there is
     *         no default value in the field or the default value could not be parsed as a long, or there is a key-value pair in
     *         the configuration but the value could not be parsed as an integer value
     */
    default int getInteger(Field field, int defaultValue) {
        return getInteger(field.name(), defaultValue);
    }

    /**
     * Get the long value associated with the given field, returning the field's default value if there is no such
     * key-value pair.
     * 
     * @param field the field
     * @param defaultValue the default value
     * @return the integer value, or null if the key is null, there is no such key-value pair in the configuration and there is
     *         no default value in the field or the default value could not be parsed as a long, or there is a key-value pair in
     *         the configuration but the value could not be parsed as a long value
     */
    default long getLong(Field field, long defaultValue) {
        return getLong(field.name(), defaultValue);
    }

    /**
     * Get the boolean value associated with the given field, returning the field's default value if there is no such
     * key-value pair.
     * 
     * @param field the field
     * @param defaultValue the default value
     * @return the boolean value, or null if the key is null, there is no such key-value pair in the configuration and there is
     *         no default value in the field or the default value could not be parsed as a long, or there is a key-value pair in
     *         the configuration but the value could not be parsed as a boolean value
     */
    default boolean getBoolean(Field field, boolean defaultValue) {
        return getBoolean(field.name(), defaultValue);
    }

    /**
     * Get an instance of the class given by the value in the configuration associated with the given key.
     * 
     * @param key the key for the configuration property
     * @param type the Class of which the resulting object is expected to be an instance of; may not be null
     * @return the new instance, or null if there is no such key-value pair in the configuration or if there is a key-value
     *         configuration but the value could not be converted to an existing class with a zero-argument constructor
     */
    default <T> T getInstance(String key, Class<T> type) {
        return getInstance(key, type, () -> getClass().getClassLoader());
    }

    /**
     * Get an instance of the class given by the value in the configuration associated with the given key.
     * 
     * @param key the key for the configuration property
     * @param type the Class of which the resulting object is expected to be an instance of; may not be null
     * @param classloaderSupplier the supplier of the ClassLoader to be used to load the resulting class; may be null if this
     *            class' ClassLoader should be used
     * @return the new instance, or null if there is no such key-value pair in the configuration or if there is a key-value
     *         configuration but the value could not be converted to an existing class with a zero-argument constructor
     */
    @SuppressWarnings("unchecked")
    default <T> T getInstance(String key, Class<T> type, Supplier<ClassLoader> classloaderSupplier) {
        String className = getString(key);
        if (className != null) {
            ClassLoader classloader = classloaderSupplier != null ? classloaderSupplier.get() : getClass().getClassLoader();
            try {
                Class<? extends T> clazz = (Class<? extends T>)classloader.loadClass(className);
                return clazz.newInstance();
            } catch (ClassNotFoundException e) {
                LoggerFactory.getLogger(getClass()).error("Unable to find class {}",className,e);
            } catch (InstantiationException e) {
                LoggerFactory.getLogger(getClass()).error("Unable to instantiate class {}",className,e);
            } catch (IllegalAccessException e) {
                LoggerFactory.getLogger(getClass()).error("Unable to access class {}",className,e);
            }
        }
        return null;
    }

    /**
     * Get an instance of the class given by the value in the configuration associated with the given field.
     * 
     * @param field the field for the configuration property
     * @param clazz the Class of which the resulting object is expected to be an instance of; may not be null
     * @return the new instance, or null if there is no such key-value pair in the configuration or if there is a key-value
     *         configuration but the value could not be converted to an existing class with a zero-argument constructor
     */
    default <T> T getInstance(Field field, Class<T> clazz) {
        return getInstance(field, clazz, () -> getClass().getClassLoader());
    }
    /**
     * Get an instance of the class given by the value in the configuration associated with the given field.
     * 
     * @param field the field for the configuration property
     * @param type the Class of which the resulting object is expected to be an instance of; may not be null
     * @param classloaderSupplier the supplier of the ClassLoader to be used to load the resulting class; may be null if this
     *            class' ClassLoader should be used
     * @return the new instance, or null if there is no such key-value pair in the configuration or if there is a key-value
     *         configuration but the value could not be converted to an existing class with a zero-argument constructor
     */
    @SuppressWarnings("unchecked")
    default <T> T getInstance(Field field, Class<T> type, Supplier<ClassLoader> classloaderSupplier) {
        String className = getString(field);
        if (className != null) {
            ClassLoader classloader = classloaderSupplier != null ? classloaderSupplier.get() : getClass().getClassLoader();
            try {
                Class<? extends T> clazz = (Class<? extends T>)classloader.loadClass(className);
                return clazz.newInstance();
            } catch (ClassNotFoundException e) {
                LoggerFactory.getLogger(getClass()).error("Unable to find class {}",className,e);
            } catch (InstantiationException e) {
                LoggerFactory.getLogger(getClass()).error("Unable to instantiate class {}",className,e);
            } catch (IllegalAccessException e) {
                LoggerFactory.getLogger(getClass()).error("Unable to access class {}",className,e);
            }
        }
        return null;
    }

    /**
     * Return a new {@link Configuration} that contains only the subset of keys that match the given prefix.
     * If desired, the keys in the resulting Configuration will have the prefix (plus any terminating "{@code .}" character if
     * needed) removed.
     * <p>
     * This method returns this Configuration instance if the supplied {@code prefix} is null or empty.
     * 
     * @param prefix the prefix
     * @param removePrefix true if the prefix (and any subsequent "{@code .}" character) should be removed from the keys in the
     *            resulting Configuration, or false if the keys in this Configuration should be used as-is in the resulting
     *            Configuration
     * @return the subset of this Configuration; never null
     */
    default Configuration subset(String prefix, boolean removePrefix) {
        if (prefix == null) return this;
        prefix = prefix.trim();
        if (prefix.isEmpty()) return this;
        String prefixWithSeparator = prefix.endsWith(".") ? prefix : prefix + ".";
        int minLength = prefixWithSeparator.length();
        Function<String, String> prefixRemover = removePrefix ? key -> key.substring(minLength) : key -> key;
        return filter(key -> key.startsWith(prefixWithSeparator)).map(prefixRemover);
    }

    /**
     * Return a new {@link Configuration} that contains only the subset of keys that satisfy the given predicate.
     * 
     * @param mapper that function that transforms keys
     * @return the subset Configuration; never null
     */
    default Configuration map(Function<String, String> mapper) {
        if (mapper == null) return this;
        Map<String, String> newToOld = new HashMap<>();
        keys().stream().filter(k -> k != null).forEach(oldKey -> {
            String newKey = mapper.apply(oldKey);
            if (newKey != null) {
                newToOld.put(newKey, oldKey);
            }
        });
        return new Configuration() {
            @Override
            public Set<String> keys() {
                return Collect.unmodifiableSet(newToOld.keySet());
            }

            @Override
            public String getString(String key) {
                String oldKey = newToOld.get(key);
                return Configuration.this.getString(oldKey);
            }
            
            @Override
            public String toString() {
                return asProperties().toString();
            }
        };
    }

    /**
     * Return a new {@link Configuration} that contains only the subset of keys that satisfy the given predicate.
     * 
     * @param matcher the function that determines whether a key should be included in the subset
     * @return the subset Configuration; never null
     */
    default Configuration filter(Predicate<? super String> matcher) {
        if (matcher == null) return this;
        return new Configuration() {
            @Override
            public Set<String> keys() {
                return Collect.unmodifiableSet(Configuration.this.keys().stream()
                                                                 .filter(k -> k != null)
                                                                 .filter(matcher)
                                                                 .collect(Collectors.toSet()));
            }

            @Override
            public String getString(String key) {
                return matcher.test(key) ? Configuration.this.getString(key) : null;
            }
            
            @Override
            public String toString() {
                return asProperties().toString();
            }
        };
    }

    /**
     * Determine if this configuration is empty and has no properties.
     * 
     * @return {@code true} if empty, or {@code false} otherwise
     */
    default boolean isEmpty() {
        return keys().isEmpty();
    }
    
    /**
     * Get a copy of these configuration properties as a Properties object.
     * 
     * @return the properties object; never null
     */
    default Properties asProperties() {
        Properties props = new Properties();
        keys().forEach(key -> {
            String value = getString(key);
            if (key != null && value != null) props.setProperty(key, value);
        });
        return props;
    }
    
    /**
     * Get a copy of these configuration properties as a Properties object.
     * 
     * @return the properties object; never null
     */
    default Map<String,String> asMap() {
        Map<String, String> props = new HashMap<>();
        keys().forEach(key -> {
            String value = getString(key);
            if (key != null && value != null) props.put(key, value);
        });
        return props;
    }

    /**
     * Return a copy of this configuration except where acceptable system properties are used to overwrite properties copied from
     * this configuration. All system properties whose name has the given prefix are added, where the prefix is removed from the
     * system property name, it is converted to lower case, and each underscore character ('{@code _}') are replaced with a
     * period ('{@code .}').
     * 
     * @param prefix the required prefix for the system properties
     * @return the resulting properties converted from the system properties; never null, but possibly empty
     */
    default Configuration withSystemProperties(String prefix) {
        int prefixLength = prefix.length();
        return withSystemProperties(input -> {
            if (input.startsWith(prefix)) {
                String withoutPrefix = input.substring(prefixLength).trim();
                if (withoutPrefix.length() > 0) {
                    // Convert to a properties format ...
                    return withoutPrefix.toLowerCase().replaceAll("[_]", ".");
                }
            }
            return null;
        });
    }

    /**
     * Return a copy of this configuration except where acceptable system properties are used to overwrite properties copied from
     * this configuration. Each of the system properties is examined and passed to the supplied function; if the result of the
     * function is a non-null string, then a property with that string as the name and the system property value are added to the
     * returned configuration.
     * 
     * @param propertyNameConverter the function that will convert the name of each system property to an applicable property name
     *            (or null if the system property name does not apply); may not be null
     * @return the resulting properties filtered from the input properties; never null, but possibly empty
     */
    default Configuration withSystemProperties(Function<String, String> propertyNameConverter) {
        Properties props = asProperties();
        Properties systemProperties = System.getProperties();
        for (String key : systemProperties.stringPropertyNames()) {
            String propName = propertyNameConverter.apply(key);
            if (propName != null && propName.length() > 0) {
                String value = systemProperties.getProperty(key);
                props.setProperty(propName, value);
            }
        }
        return from(props);
    }

    /**
     * Validate the supplied fields in this configuration.
     * 
     * @param fields the fields
     * @param problems the consumer to be called with each problem; never null
     * @return {@code true} if the value is considered valid, or {@code false} if it is not valid
     */
    default boolean validate(Iterable<Field> fields, Consumer<String> problems) {
        boolean valid = true;
        for ( Field field : fields ) {
            if ( !field.validate(this,problems) ) valid = false;
        }
        return valid;
    }

    /**
     * Validate the supplied fields in this configuration.
     * @param fields the fields
     * @param problems the consumer to be called with each problem; never null
     * @return {@code true} if the value is considered valid, or {@code false} if it is not valid
     */
    default boolean validate(Field[] fields, Consumer<String> problems) {
        boolean valid = true;
        for ( Field field : fields ) {
            if ( !field.validate(this,problems) ) valid = false;
        }
        return valid;
    }
}