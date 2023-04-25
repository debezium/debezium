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
import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.config.Field.ValidationOutput;
import io.debezium.util.Collect;
import io.debezium.util.IoUtil;
import io.debezium.util.Strings;

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

    Logger CONFIGURATION_LOGGER = LoggerFactory.getLogger(Configuration.class);

    Pattern PASSWORD_PATTERN = Pattern.compile(".*password$|.*sasl\\.jaas\\.config$|.*basic\\.auth\\.user\\.info|.*registry\\.auth\\.client-secret",
            Pattern.CASE_INSENSITIVE);

    /**
     * The basic interface for configuration builders.
     *
     * @param <C> the type of configuration
     * @param <B> the type of builder
     */
    interface ConfigBuilder<C extends Configuration, B extends ConfigBuilder<C, B>> {
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
        default B with(String key, EnumeratedValue value) {
            return with(key, value != null ? value.getValue() : null);
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
         * Associate the given class name value with the specified key.
         *
         * @param key the key
         * @param value the Class value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(String key, Class<?> value) {
            return with(key, value != null ? value.getName() : null);
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
        default B withDefault(String key, EnumeratedValue value) {
            return withDefault(key, value != null ? value.getValue() : null);
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
         * If there is no field with the specified key, then associate the given class name value with the specified key.
         *
         * @param key the key
         * @param value the Class value
         * @return this builder object so methods can be chained together; never null
         */
        default B withDefault(String key, Class<?> value) {
            return withDefault(key, value != null ? value.getName() : null);
        }

        /**
         * If any of the fields in the supplied Configuration object do not exist, then add them.
         *
         * @param other the configuration whose fields should be added; may not be null
         * @return this builder object so methods can be chained together; never null
         */
        default B withDefault(Configuration other) {
            return apply(builder -> other.forEach(builder::withDefault));
        }

        /**
         * Add all of the fields in the supplied Configuration object.
         *
         * @param other the configuration whose fields should be added; may not be null
         * @return this builder object so methods can be chained together; never null
         */
        default B with(Configuration other) {
            return apply(builder -> other.forEach(builder::with));
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
        default B with(Field field, Object value) {
            return with(field.name(), value);
        }

        /**
         * Associate the given value with the key of the specified field.
         *
         * @param field the predefined field for the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(Field field, EnumeratedValue value) {
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
         * Associate the given class name value with the specified field.
         *
         * @param field the predefined field for the key
         * @param value the Class value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(Field field, Class<?> value) {
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
        default B withDefault(Field field, Object value) {
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
         * If the field does not have a value, then associate the given value with the key of the specified field.
         *
         * @param field the predefined field for the key
         * @param value the default value
         * @return this builder object so methods can be chained together; never null
         */
        default B withDefault(Field field, Class<?> value) {
            return withDefault(field.name(), value != null ? value.getName() : null);
        }

        /**
         * Apply the function to this builder.
         *
         * @param function the predefined field for the key
         * @return this builder object so methods can be chained together; never null
         */
        B apply(Consumer<B> function);

        /**
         * Apply the function to this builder to change a potentially existing boolean field.
         *
         * @param key the key
         * @param function the function that computes the new value given a possibly-existing value; may not be null
         * @return this builder object so methods can be chained together; never null
         * @throws NumberFormatException if the existing value is not a boolean
         */
        default B changeBoolean(String key, Function<Boolean, Boolean> function) {
            Function<String, String> strFunction = (existingStr) -> {
                Boolean result = function.apply(existingStr != null ? Boolean.valueOf(existingStr) : null);
                return result != null ? result.toString() : null;
            };
            return changeString(key, strFunction);
        }

        /**
         * Apply the function to this builder to change a potentially existing string field.
         *
         * @param key the key
         * @param function the function that computes the new value given a possibly-existing value; may not be null
         * @return this builder object so methods can be chained together; never null
         */
        B changeString(String key, Function<String, String> function);

        /**
         * Apply the function to this builder to change a potentially existing double field.
         *
         * @param key the key
         * @param function the function that computes the new value given a possibly-existing value; may not be null
         * @return this builder object so methods can be chained together; never null
         * @throws NumberFormatException if the existing value is not a double
         */
        default B changeDouble(String key, Function<Double, Double> function) {
            Function<String, String> strFunction = (existingStr) -> {
                Double result = function.apply(existingStr != null ? Double.valueOf(existingStr) : null);
                return result != null ? result.toString() : null;
            };
            return changeString(key, strFunction);
        }

        /**
         * Apply the function to this builder to change a potentially existing float field.
         *
         * @param key the key
         * @param function the function that computes the new value given a possibly-existing value; may not be null
         * @return this builder object so methods can be chained together; never null
         * @throws NumberFormatException if the existing value is not a float
         */
        default B changeFloat(String key, Function<Float, Float> function) {
            Function<String, String> strFunction = (existingStr) -> {
                Float result = function.apply(existingStr != null ? Float.valueOf(existingStr) : null);
                return result != null ? result.toString() : null;
            };
            return changeString(key, strFunction);
        }

        /**
         * Apply the function to this builder to change a potentially existing long field.
         *
         * @param key the key
         * @param function the function that computes the new value given a possibly-existing value; may not be null
         * @return this builder object so methods can be chained together; never null
         * @throws NumberFormatException if the existing value is not a long
         */
        default B changeLong(String key, Function<Long, Long> function) {
            Function<String, String> strFunction = (existingStr) -> {
                Long result = function.apply(existingStr != null ? Long.valueOf(existingStr) : null);
                return result != null ? result.toString() : null;
            };
            return changeString(key, strFunction);
        }

        /**
         * Apply the function to this builder to change a potentially existing integer field.
         *
         * @param key the key
         * @param function the function that computes the new value given a possibly-existing value; may not be null
         * @return this builder object so methods can be chained together; never null
         * @throws NumberFormatException if the existing value is not an integer
         */
        default B changeInteger(String key, Function<Integer, Integer> function) {
            Function<String, String> strFunction = (existingStr) -> {
                Integer result = function.apply(existingStr != null ? Integer.valueOf(existingStr) : null);
                return result != null ? result.toString() : null;
            };
            return changeString(key, strFunction);
        }

        /**
         * Apply the function to this builder to change a potentially existing boolean field.
         *
         * @param field the predefined field for the key
         * @param function the function that computes the new value given a possibly-existing value; may not be null
         * @return this builder object so methods can be chained together; never null
         */
        default B changeBoolean(Field field, Function<Boolean, Boolean> function) {
            Function<String, String> strFunction = (existingStr) -> {
                Boolean result = function.apply(existingStr != null ? Boolean.valueOf(existingStr) : null);
                return result != null ? result.toString() : null;
            };
            return changeString(field, strFunction);
        }

        /**
         * Apply the function to this builder to change a potentially existing string field.
         *
         * @param field the predefined field for the key
         * @param function the function that computes the new value given a possibly-existing value; may not be null
         * @return this builder object so methods can be chained together; never null
         */
        B changeString(Field field, Function<String, String> function);

        /**
         * Apply the function to this builder to change a potentially existing double field.
         *
         * @param field the predefined field for the key
         * @param function the function that computes the new value given a possibly-existing value; may not be null
         * @return this builder object so methods can be chained together; never null
         */
        default B changeDouble(Field field, Function<Double, Double> function) {
            Function<String, String> strFunction = (existingStr) -> {
                Double result = function.apply(existingStr != null ? Double.valueOf(existingStr) : null);
                return result != null ? result.toString() : null;
            };
            return changeString(field, strFunction);
        }

        /**
         * Apply the function to this builder to change a potentially existing float field.
         *
         * @param field the predefined field for the key
         * @param function the function that computes the new value given a possibly-existing value; may not be null
         * @return this builder object so methods can be chained together; never null
         */
        default B changeFloat(Field field, Function<Float, Float> function) {
            Function<String, String> strFunction = (existingStr) -> {
                Float result = function.apply(existingStr != null ? Float.valueOf(existingStr) : null);
                return result != null ? result.toString() : null;
            };
            return changeString(field, strFunction);
        }

        /**
         * Apply the function to this builder to change a potentially existing long field.
         *
         * @param field the predefined field for the key
         * @param function the function that computes the new value given a possibly-existing value; may not be null
         * @return this builder object so methods can be chained together; never null
         */
        default B changeLong(Field field, Function<Long, Long> function) {
            Function<String, String> strFunction = (existingStr) -> {
                Long result = function.apply(existingStr != null ? Long.valueOf(existingStr) : null);
                return result != null ? result.toString() : null;
            };
            return changeString(field, strFunction);
        }

        /**
         * Apply the function to this builder to change a potentially existing integer field.
         *
         * @param field the predefined field for the key
         * @param function the function that computes the new value given a possibly-existing value; may not be null
         * @return this builder object so methods can be chained together; never null
         */
        default B changeInteger(Field field, Function<Integer, Integer> function) {
            Function<String, String> strFunction = (existingStr) -> {
                Integer result = function.apply(existingStr != null ? Integer.valueOf(existingStr) : null);
                return result != null ? result.toString() : null;
            };
            return changeString(field, strFunction);
        }

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
    class Builder implements ConfigBuilder<Configuration, Builder> {
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
            if (!props.containsKey(key)) {
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
        public Builder changeString(String key, Function<String, String> function) {
            return changeString(key, null, function);
        }

        @Override
        public Builder changeString(Field field, Function<String, String> function) {
            return changeString(field.name(), field.defaultValueAsString(), function);
        }

        protected Builder changeString(String key, String defaultValue, Function<String, String> function) {
            String existing = props.getProperty(key);
            if (existing == null) {
                existing = defaultValue;
            }
            String newValue = function.apply(existing);
            return with(key, newValue);
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
    static Builder create() {
        return new Builder();
    }

    /**
     * Create a new {@link Builder configuration builder} that starts with a copy of the supplied configuration.
     *
     * @param config the configuration to copy; may be null
     * @return the configuration builder
     */
    static Builder copy(Configuration config) {
        return config != null ? new Builder(config.asProperties()) : new Builder();
    }

    /**
     * Create a Configuration object that is populated by system properties, per {@link #withSystemProperties(String)}.
     *
     * @param prefix the required prefix for the system properties; may not be null but may be empty
     * @return the configuration
     */
    static Configuration fromSystemProperties(String prefix) {
        return empty().withSystemProperties(prefix);
    }

    /**
     * Obtain an empty configuration.
     *
     * @return an empty configuration; never null
     */
    static Configuration empty() {
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
    static Configuration from(Properties properties) {
        Properties props = new Properties();
        if (properties != null) {
            props.putAll(properties);
        }
        return new Configuration() {
            @Override
            public String getString(String key) {
                return props.getProperty(key);
            }

            @Override
            public Set<String> keys() {
                return props.stringPropertyNames();
            }

            @Override
            public String toString() {
                return withMaskedPasswords().asProperties().toString();
            }
        };
    }

    /**
     * Obtain a configuration instance by copying the supplied map of string keys and object values. The entries within the map
     * are copied so that the resulting Configuration cannot be modified.
     *
     * @param properties the properties; may be null or empty
     * @return the configuration; never null
     */
    static Configuration from(Map<String, ?> properties) {
        return from(properties, value -> {
            if (value == null) {
                return null;
            }
            if (value instanceof Collection<?>) {
                return Strings.join(",", (List<?>) value);
            }
            return value.toString();
        });
    }

    /**
     * Obtain a configuration instance by copying the supplied map of string keys and object values. The entries within the map
     * are copied so that the resulting Configuration cannot be modified.
     *
     * @param properties the properties; may be null or empty
     * @param conversion the function that converts the supplied values into strings, or returns {@code null} if the value
     *            is to be excluded
     * @return the configuration; never null
     */
    static <T> Configuration from(Map<String, T> properties, Function<T, String> conversion) {
        Map<String, T> props = new HashMap<>();
        if (properties != null) {
            props.putAll(properties);
        }
        return new Configuration() {
            @Override
            public String getString(String key) {
                return conversion.apply((T) props.get(key));
            }

            @Override
            public Set<String> keys() {
                return props.keySet();
            }

            @Override
            public String toString() {
                return withMaskedPasswords().asProperties().toString();
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
    static Configuration load(URL url) throws IOException {
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
    static Configuration load(File file) throws IOException {
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
    static Configuration load(InputStream stream) throws IOException {
        try {
            Properties properties = new Properties();
            properties.load(stream);
            return from(properties);
        }
        finally {
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
    static Configuration load(Reader reader) throws IOException {
        try {
            Properties properties = new Properties();
            properties.load(reader);
            return from(properties);
        }
        finally {
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
    static Configuration load(String path, Class<?> clazz) throws IOException {
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
    static Configuration load(String path, ClassLoader classLoader) throws IOException {
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
    static Configuration load(String path, ClassLoader classLoader, Consumer<String> logger) throws IOException {
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
     *
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
     * Determine whether this configuration contains a key-value pair associated with the given field and the value
     * is non-null.
     *
     * @param field the field; may not be null
     * @return true if the configuration contains the key, or false otherwise
     */
    default boolean hasKey(Field field) {
        return hasKey(field.name());
    }

    /**
     * Get the set of keys in this configuration.
     *
     * @return the set of keys; never null but possibly empty
     */
    Set<String> keys();

    /**
     * Get the string value associated with the given key.
     *
     * @param key the key for the configuration property
     * @return the value, or null if the key is null or there is no such key-value pair in the configuration
     */
    String getString(String key);

    default List<String> getList(Field field) {
        return getList(field.name());
    }

    default List<String> getList(String key) {
        return getList(key, ",", Function.identity());
    }

    default <T> List<T> getList(Field field, String separator, Function<String, T> converter) {
        return getList(field.name(), separator, converter);
    }

    default <T> List<T> getList(String key, String separator, Function<String, T> converter) {
        var value = getString(key);
        return Arrays.stream(value.split(separator))
                .map(String::trim)
                .map(converter)
                .collect(Collectors.toList());
    }

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
        return getString(field.name(), field.defaultValueAsString());
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
        return getString(field.name(), () -> {
            String value = field.defaultValueAsString();
            return value != null ? value : defaultValue;
        });
    }

    /**
     * Get the string value(s) associated with the given key, where the supplied regular expression is used to parse the single
     * string value into multiple values.
     *
     * @param field the field; may not be null
     * @param regex the delimiting regular expression
     * @return the list of string values; null only if there is no such key-value pair in the configuration
     * @see String#split(String)
     */
    default List<String> getStrings(Field field, String regex) {
        return getStrings(field.name(), regex);
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
        if (value == null) {
            return null;
        }
        return Collect.arrayListOf(value.split(regex));
    }

    /**
     * Get the string value(s) associated with the given key, where the supplied regular expression is used to parse the single
     * string value into multiple values. In addition, all values will be trimmed.
     *
     * @param field the field for the configuration property
     * @param regex the delimiting regular expression
     * @return the list of string values; null only if there is no such key-value pair in the configuration
     * @see String#split(String)
     */
    default List<String> getTrimmedStrings(Field field, String regex) {
        String value = getString(field);
        if (value == null) {
            return null;
        }

        return Arrays.stream(value.split(regex))
                .map(String::trim)
                .collect(Collectors.toList());
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
    default Number getNumber(String key, Supplier<Number> defaultValueSupplier) {
        String value = getString(key);
        return Strings.asNumber(value, defaultValueSupplier);
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
                return Integer.valueOf(value);
            }
            catch (NumberFormatException e) {
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
                return Long.valueOf(value);
            }
            catch (NumberFormatException e) {
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
            if (Boolean.valueOf(value)) {
                return Boolean.TRUE;
            }
            if (value.equals("false")) {
                return false;
            }
        }
        return defaultValueSupplier != null ? defaultValueSupplier.getAsBoolean() : null;
    }

    /**
     * Get the numeric value associated with the given field, returning the field's default value if there is no such
     * key-value pair.
     *
     * @param field the field
     * @return the integer value, or null if the key is null, there is no such key-value pair in the configuration and there is
     *         no default value in the field or the default value could not be parsed as a long, or there is a key-value pair in
     *         the configuration but the value could not be parsed as an integer value
     * @throws NumberFormatException if there is no name-value pair and the field has no default value
     */
    default Number getNumber(Field field) {
        return getNumber(field.name(), () -> Strings.asNumber(field.defaultValueAsString()));
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
        return getInteger(field.name(), () -> Integer.valueOf(field.defaultValueAsString())).intValue();
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
        return getLong(field.name(), () -> Long.valueOf(field.defaultValueAsString())).longValue();
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
        return getBoolean(field.name(), () -> Boolean.valueOf(field.defaultValueAsString())).booleanValue();
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
     * Get the integer value associated with the given key, using the given supplier to obtain a default value if there is no such
     * key-value pair.
     *
     * @param field the field
     * @param defaultValueSupplier the supplier for the default value; may be null
     * @return the integer value, or null if the key is null, there is no such key-value pair in the configuration, the
     *         {@code defaultValueSupplier} reference is null, or there is a key-value pair in the configuration but the value
     *         could not be parsed as an integer
     */
    default Integer getInteger(Field field, IntSupplier defaultValueSupplier) {
        return getInteger(field.name(), defaultValueSupplier);
    }

    /**
     * Get the long value associated with the given key, using the given supplier to obtain a default value if there is no such
     * key-value pair.
     *
     * @param field the field
     * @param defaultValueSupplier the supplier for the default value; may be null
     * @return the long value, or null if the key is null, there is no such key-value pair in the configuration, the
     *         {@code defaultValueSupplier} reference is null, or there is a key-value pair in the configuration but the value
     *         could not be parsed as a long
     */
    default Long getLong(Field field, LongSupplier defaultValueSupplier) {
        return getLong(field.name(), defaultValueSupplier);
    }

    /**
     * Get the boolean value associated with the given key, using the given supplier to obtain a default value if there is no such
     * key-value pair.
     *
     * @param field the field
     * @param defaultValueSupplier the supplier for the default value; may be null
     * @return the boolean value, or null if the key is null, there is no such key-value pair in the configuration, the
     *         {@code defaultValueSupplier} reference is null, or there is a key-value pair in the configuration but the value
     *         could not be parsed as a boolean value
     */
    default Boolean getBoolean(Field field, BooleanSupplier defaultValueSupplier) {
        return getBoolean(field.name(), defaultValueSupplier);
    }

    /**
     * Get the boolean value associated with the given key, using the given supplier to obtain a default value if there is no such
     * key-value pair.
     *
     * @param field the field
     * @param defaultValueSupplier the supplier of value that should be returned by default if there is no such key-value pair in
     *            the configuration; may be null and may return null
     * @return the configuration value, or the {@code defaultValue} if there is no such key-value pair in the configuration
     */
    default String getString(Field field, Supplier<String> defaultValueSupplier) {
        return getString(field.name(), defaultValueSupplier);
    }

    /**
     *
     * Gets the duration value associated with the given key.
     *
     * @param field the field
     * @param unit the temporal unit of the duration value
     * @return the duration value associated with the given key
     */
    default Duration getDuration(Field field, TemporalUnit unit) {
        return Duration.of(getLong(field), unit);
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
        return Instantiator.getInstance(getString(key));
    }

    /**
     * Get an instance of the class given by the value in the configuration associated with the given key.
     * The instance is created using {@code Instance(Configuration)} constructor.
     *
     * @param key the key for the configuration property
     * @param clazz the Class of which the resulting object is expected to be an instance of; may not be null
     * @param configuration {@link Configuration} object that is passed as a parameter to the constructor
     * @return the new instance, or null if there is no such key-value pair in the configuration or if there is a key-value
     *         configuration but the value could not be converted to an existing class with a zero-argument constructor
     */
    default <T> T getInstance(String key, Class<T> clazz, Configuration configuration) {
        return Instantiator.getInstance(getString(key), configuration);
    }

    /**
     * Get an instance of the class given by the value in the configuration associated with the given field.
     *
     * @param field the field for the configuration property
     * @param type the Class of which the resulting object is expected to be an instance of; may not be null
     * @return the new instance, or null if there is no such key-value pair in the configuration or if there is a key-value
     *         configuration but the value could not be converted to an existing class with a zero-argument constructor
     */
    default <T> T getInstance(Field field, Class<T> type) {
        return Instantiator.getInstance(getString(field));
    }

    /**
     * Get an instance of the class given by the value in the configuration associated with the given field.
     * The instance is created using {@code Instance(Configuration)} constructor.
     *
     * @param field the field for the configuration property
     * @param clazz the Class of which the resulting object is expected to be an instance of; may not be null
     * @param configuration the {@link Configuration} object that is passed as a parameter to the constructor
     * @return the new instance, or null if there is no such key-value pair in the configuration or if there is a key-value
     *         configuration but the value could not be converted to an existing class with a zero-argument constructor
     */
    default <T> T getInstance(Field field, Class<T> clazz, Configuration configuration) {
        return Instantiator.getInstance(getString(field), configuration);
    }

    /**
     * Get an instance of the class given by the value in the configuration associated with the given field.
     * The instance is created using {@code Instance(Configuration)} constructor.
     *
     * @param field the field for the configuration property
     * @param clazz the Class of which the resulting object is expected to be an instance of; may not be null
     * @param props the {@link Properties} object that is passed as a parameter to the constructor
     * @return the new instance, or null if there is no such key-value pair in the configuration or if there is a key-value
     *         configuration but the value could not be converted to an existing class with a zero-argument constructor
     */
    default <T> T getInstance(Field field, Class<T> clazz, Properties props) {
        return Instantiator.getInstanceWithProperties(getString(field), props);
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
        if (prefix == null) {
            return this;
        }
        prefix = prefix.trim();
        if (prefix.isEmpty()) {
            return this;
        }
        String prefixWithSeparator = prefix.endsWith(".") ? prefix : prefix + ".";
        int minLength = prefixWithSeparator.length();
        Function<String, String> prefixRemover = removePrefix ? key -> key.substring(minLength) : key -> key;
        return filter(key -> key != null && key.startsWith(prefixWithSeparator)).map(prefixRemover);
    }

    /**
     * Return a new {@link Configuration} that merges several {@link Configuration}s into one.
     * <p>
     * This method returns this Configuration instance if the supplied {@code configs} is null or empty.
     *
     * @param configs Configurations to be merged
     * @return the subset of this Configuration; never null
     */
    default Configuration merge(Configuration... configs) {
        if (configs == null || configs.length == 0) {
            return this;
        }

        Set<String> keys = new HashSet<>(keys());
        for (Configuration config : configs) {
            keys.addAll(config.keys());
        }

        return new Configuration() {
            @Override
            public Set<String> keys() {
                return Collect.unmodifiableSet(keys.stream().filter(k -> k != null).collect(Collectors.toSet()));
            }

            @Override
            public String getString(String key) {
                String value = null;
                for (Configuration config : Collect.arrayListOf(Configuration.this, configs)) {
                    value = config.getString(key);
                    if (value != null) {
                        break;
                    }
                }
                return value;
            }

            @Override
            public String toString() {
                return withMaskedPasswords().asProperties().toString();
            }
        };
    }

    /**
     * Return a new {@link Configuration} that contains only the subset of keys that satisfy the given predicate.
     *
     * @param mapper that function that transforms keys
     * @return the subset Configuration; never null
     */
    default Configuration map(Function<String, String> mapper) {
        if (mapper == null) {
            return this;
        }
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
                return withMaskedPasswords().asProperties().toString();
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
        if (matcher == null) {
            return this;
        }
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
                return withMaskedPasswords().asProperties().toString();
            }
        };
    }

    /**
     * Return a new {@link Configuration} that contains the mapped values.
     *
     * @param mapper the function that takes a key and value and returns the new mapped value
     * @return the Configuration with mapped values; never null
     */
    default Configuration mapped(BiFunction<? super String, ? super String, String> mapper) {
        if (mapper == null) {
            return this;
        }
        return new Configuration() {
            @Override
            public Set<String> keys() {
                return Configuration.this.keys();
            }

            @Override
            public String getString(String key) {
                return mapper.apply(key, Configuration.this.getString(key));
            }

            @Override
            public String toString() {
                return withMaskedPasswords().asProperties().toString();
            }
        };
    }

    /**
     * Return a new {@link Configuration} that contains all of the same fields as this configuration, except with all
     * variables in the fields replaced with values from the supplied function. Variables found in fields will be left as-is
     * if the supplied function returns no value for the variable name(s).
     * <p>
     * Variables may appear anywhere within a field value, and multiple variables can be used within the same field. Variables
     * take the form:
     *
     * <pre>
     *    variable := '${' variableNames [ ':' defaultValue ] '}'
     *    variableNames := variableName [ ',' variableNames ]
     *    variableName := // any characters except ',' and ':' and '}'
     *    defaultValue := // any characters except '}'
     * </pre>
     *
     * and examples of variables include:
     * <ul>
     * <li><code>${var1}</code></li>
     * <li><code>${var1:defaultValue}</code></li>
     * <li><code>${var1,var2}</code></li>
     * <li><code>${var1,var2:defaultValue}</code></li>
     * </ul>
     *
     * The <i>variableName</i> literal is the name used to look up a the property.
     * </p>
     * This syntax supports multiple <i>variables</i>. The logic will process the <i>variables</i> from let to right,
     * until an existing property is found. And at that point, it will stop and will not attempt to find values for the other
     * <i>variables</i>.
     * <p>
     *
     *
     * @param valuesByVariableName the function that returns a variable value for a variable name; may not be null but may
     *            return null if the variable name is not known
     * @return the Configuration with masked values for matching keys; never null
     */
    default Configuration withReplacedVariables(Function<String, String> valuesByVariableName) {
        return mapped((key, value) -> {
            return Strings.replaceVariables(value, valuesByVariableName);
        });
    }

    /**
     * Return a new {@link Configuration} that contains all of the same fields as this configuration, except with masked values
     * for all keys that end in "password".
     *
     * @return the Configuration with masked values for matching keys; never null
     */
    default Configuration withMaskedPasswords() {
        return withMasked(PASSWORD_PATTERN);
    }

    /**
     * Return a new {@link Configuration} that contains all of the same fields as this configuration, except with masked values
     * for all keys that match the specified pattern.
     *
     * @param keyRegex the regular expression to match against the keys
     * @return the Configuration with masked values for matching keys; never null
     */
    default Configuration withMasked(String keyRegex) {
        if (keyRegex == null) {
            return this;
        }
        return withMasked(Pattern.compile(keyRegex));
    }

    /**
     * Return a new {@link Configuration} that contains all of the same fields as this configuration, except with masked values
     * for all keys that match the specified pattern.
     *
     * @param keyRegex the regular expression to match against the keys
     * @return the Configuration with masked values for matching keys; never null
     */
    default Configuration withMasked(Pattern keyRegex) {
        if (keyRegex == null) {
            return this;
        }
        return new Configuration() {
            @Override
            public Set<String> keys() {
                return Configuration.this.keys();
            }

            @Override
            public String getString(String key) {
                boolean matches = keyRegex.matcher(key).matches();
                return matches ? "********" : Configuration.this.getString(key);
            }

            @Override
            public String toString() {
                return withMaskedPasswords().asProperties().toString();
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
        return asProperties(null);
    }

    /**
     * Get a copy of these configuration properties as a Properties object.
     *
     * @param fields the fields defining the defaults; may be null
     * @return the properties object; never null
     */
    default Properties asProperties(Field.Set fields) {
        Properties props = new Properties();
        // Add all values as-is ...
        keys().forEach(key -> {
            String value = getString(key);
            if (key != null && value != null) {
                props.setProperty(key, value);
            }
        });
        if (fields != null) {
            // Add the default values ...
            fields.forEach(field -> {
                props.put(field.name(), getString(field));
            });
        }
        return props;
    }

    /**
     * Get a copy of these configuration properties as a Properties object.
     *
     * @return the properties object; never null
     */
    default Map<String, String> asMap() {
        return asMap(null);
    }

    /**
     * Get a copy of these configuration properties with defaults as a Map.
     *
     * @param fields the fields defining the defaults; may be null
     * @return the properties object; never null
     */
    default Map<String, String> asMap(Field.Set fields) {
        Map<String, String> props = new HashMap<>();
        // Add all values as-is ...
        keys().forEach(key -> {
            String value = getString(key);
            if (key != null && value != null) {
                props.put(key, value);
            }
        });
        if (fields != null) {
            // Add the default values ...
            fields.forEach(field -> {
                props.put(field.name(), getString(field));
            });
        }
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
     * Validate the supplied fields in this configuration. Extra fields not described by the supplied {@code fields} parameter
     * are not validated.
     *
     * @param fields the fields
     * @param problems the consumer to be called with each problem; never null
     * @return {@code true} if the value is considered valid, or {@code false} if it is not valid
     */
    default boolean validate(Iterable<Field> fields, ValidationOutput problems) {
        boolean valid = true;
        for (Field field : fields) {
            if (!field.validate(this, problems)) {
                valid = false;
            }
        }
        return valid;
    }

    /**
     * Validate the supplied fields in this configuration. Extra fields not described by the supplied {@code fields} parameter
     * are not validated.
     *
     * @param fields the fields
     * @param problems the consumer to be called with each problem; never null
     * @return {@code true} if the value is considered valid, or {@code false} if it is not valid
     */
    default boolean validateAndRecord(Iterable<Field> fields, Consumer<String> problems) {
        return validate(fields, (f, v, problem) -> {
            if (v == null) {
                problems.accept("The '" + f.name() + "' value is invalid: " + problem);
            }
            else {
                String valueStr = v.toString();
                if (v instanceof CharSequence) {
                    if (PASSWORD_PATTERN.matcher((CharSequence) v).matches()) {
                        valueStr = "********"; // mask any fields that we know are passwords
                    }
                    else {
                        valueStr = "'" + valueStr + "'";
                    }
                }
                problems.accept("The '" + f.name() + "' value " + valueStr + " is invalid: " + problem);
            }
        });
    }

    /**
     * Validate the supplied fields in this configuration. Extra fields not described by the supplied {@code fields} parameter
     * are not validated.
     *
     * @param fields the fields
     * @return the {@link ConfigValue} for each of the fields; never null
     */
    default Map<String, ConfigValue> validate(Field.Set fields) {
        // Create a map of configuration values for each field ...
        Map<String, ConfigValue> configValuesByFieldName = new HashMap<>();
        fields.forEach(field -> configValuesByFieldName.put(field.name(), new ConfigValue(field.name())));

        // If any dependents don't exist ...
        fields.forEachMissingDependent(missingDepedent -> {
            ConfigValue undefinedConfigValue = new ConfigValue(missingDepedent);
            undefinedConfigValue.addErrorMessage(missingDepedent + " is referred in the dependents, but not defined.");
            undefinedConfigValue.visible(false);
            configValuesByFieldName.put(missingDepedent, undefinedConfigValue);
        });

        // Now validate each top-level field ...
        fields.forEachTopLevelField(field -> field.validate(this, fields::fieldWithName, configValuesByFieldName));

        return configValuesByFieldName;
    }

    /**
     * Apply the given function to all fields whose names match the given regular expression.
     *
     * @param regex the regular expression string; may not be null
     * @param function the consumer that takes the name and value of matching fields; may not be null
     */
    default void forEachMatchingFieldName(String regex, BiConsumer<String, String> function) {
        forEachMatchingFieldName(Pattern.compile(regex), function);
    }

    /**
     * Apply the given function to all fields whose names match the given regular expression.
     *
     * @param regex the regular expression string; may not be null
     * @param function the consumer that takes the name and value of matching fields; may not be null
     */
    default void forEachMatchingFieldName(Pattern regex, BiConsumer<String, String> function) {
        this.asMap().forEach((fieldName, fieldValue) -> {
            if (regex.matcher(fieldName).matches()) {
                function.accept(fieldName, fieldValue);
            }
        });
    }

    /**
     * For all fields whose names match the given regular expression, extract an integer from the first group in the regular
     * expression and call the supplied function.
     *
     * @param regex the regular expression string; may not be null
     * @param function the consumer that takes the value of matching field and the integer extracted from the field name; may not
     *            be null
     */
    default <T> void forEachMatchingFieldNameWithInteger(String regex, BiConsumer<String, Integer> function) {
        forEachMatchingFieldNameWithInteger(Pattern.compile(regex), 1, function);
    }

    /**
     * For all fields whose names match the given regular expression, extract an integer from the first group in the regular
     * expression and call the supplied function.
     *
     * @param regex the regular expression string; may not be null
     * @param groupNumber the number of the regular expression group containing the integer to be extracted; must be positive
     * @param function the consumer that takes the value of matching field and the integer extracted from the field name; may not
     *            be null
     */
    default <T> void forEachMatchingFieldNameWithInteger(String regex, int groupNumber, BiConsumer<String, Integer> function) {
        forEachMatchingFieldNameWithInteger(Pattern.compile(regex), groupNumber, function);
    }

    /**
     * For all fields whose names match the given regular expression, extract an integer from the first group in the regular
     * expression and call the supplied function.
     *
     * @param regex the regular expression string; may not be null
     * @param groupNumber the number of the regular expression group containing the integer to be extracted; must be positive
     * @param function the consumer that takes the value of matching field and the integer extracted from the field name; may not
     *            be null
     */
    default <T> void forEachMatchingFieldNameWithInteger(Pattern regex, int groupNumber, BiConsumer<String, Integer> function) {
        BiFunction<String, String, Integer> extractor = (fieldName, strValue) -> {
            try {
                return Integer.valueOf(strValue);
            }
            catch (NumberFormatException e) {
                LoggerFactory.getLogger(getClass()).error("Unexpected value {} extracted from configuration field '{}' using regex '{}'",
                        strValue, fieldName, regex);
                return null;
            }
        };
        forEachMatchingFieldName(regex, groupNumber, extractor, function);
    }

    /**
     * For all fields whose names match the given regular expression, extract a boolean value from the first group in the regular
     * expression and call the supplied function.
     *
     * @param regex the regular expression string; may not be null
     * @param function the consumer that takes the value of matching field and the boolean extracted from the field name; may not
     *            be null
     */
    default <T> void forEachMatchingFieldNameWithBoolean(String regex, BiConsumer<String, Boolean> function) {
        forEachMatchingFieldNameWithBoolean(Pattern.compile(regex), 1, function);
    }

    /**
     * For all fields whose names match the given regular expression, extract a boolean value from the first group in the regular
     * expression and call the supplied function.
     *
     * @param regex the regular expression string; may not be null
     * @param groupNumber the number of the regular expression group containing the boolean to be extracted; must be positive
     * @param function the consumer that takes the value of matching field and the boolean extracted from the field name; may not
     *            be null
     */
    default <T> void forEachMatchingFieldNameWithBoolean(String regex, int groupNumber, BiConsumer<String, Boolean> function) {
        forEachMatchingFieldNameWithBoolean(Pattern.compile(regex), groupNumber, function);
    }

    /**
     * For all fields whose names match the given regular expression, extract a boolean value from the first group in the regular
     * expression and call the supplied function.
     *
     * @param regex the regular expression string; may not be null
     * @param groupNumber the number of the regular expression group containing the boolean to be extracted; must be positive
     * @param function the consumer that takes the value of matching field and the boolean extracted from the field name; may not
     *            be null
     */
    default <T> void forEachMatchingFieldNameWithBoolean(Pattern regex, int groupNumber, BiConsumer<String, Boolean> function) {
        BiFunction<String, String, Boolean> extractor = (fieldName, strValue) -> {
            try {
                return Boolean.parseBoolean(strValue);
            }
            catch (NumberFormatException e) {
                LoggerFactory.getLogger(getClass()).error("Unexpected value {} extracted from configuration field '{}' using regex '{}'",
                        strValue, fieldName, regex);
                return null;
            }
        };
        forEachMatchingFieldName(regex, groupNumber, extractor, function);
    }

    /**
     * For all fields whose names match the given regular expression, extract a string value from the first group in the regular
     * expression and call the supplied function.
     *
     * @param regex the regular expression string; may not be null
     * @param function the consumer that takes the value of matching field and the string value extracted from the field name; may
     *            not be null
     */
    default <T> void forEachMatchingFieldNameWithString(String regex, BiConsumer<String, String> function) {
        forEachMatchingFieldNameWithString(Pattern.compile(regex), 1, function);
    }

    /**
     * For all fields whose names match the given regular expression, extract a string value from the first group in the regular
     * expression and call the supplied function.
     *
     * @param regex the regular expression string; may not be null
     * @param groupNumber the number of the regular expression group containing the string value to be extracted; must be positive
     * @param function the consumer that takes the value of matching field and the string value extracted from the field name; may
     *            not be null
     */
    default <T> void forEachMatchingFieldNameWithString(String regex, int groupNumber, BiConsumer<String, String> function) {
        forEachMatchingFieldNameWithString(Pattern.compile(regex), groupNumber, function);
    }

    /**
     * For all fields whose names match the given regular expression, extract a string value from the first group in the regular
     * expression and call the supplied function.
     *
     * @param regex the regular expression string; may not be null
     * @param groupNumber the number of the regular expression group containing the string value to be extracted; must be positive
     * @param function the consumer that takes the value of matching field and the string value extracted from the field name; may
     *            not be null
     */
    default <T> void forEachMatchingFieldNameWithString(Pattern regex, int groupNumber, BiConsumer<String, String> function) {
        forEachMatchingFieldName(regex, groupNumber, (fieldName, value) -> value, function);
    }

    /**
     * For all fields whose names match the given regular expression, extract a value from the specified group in the regular
     * expression and call the supplied function.
     *
     * @param regex the regular expression string; may not be null
     * @param groupNumber the number of the regular expression group containing the string value to be extracted; must be positive
     * @param groupExtractor the function that extracts the value from the group
     * @param function the consumer that takes the value of matching field and the value extracted from the field name; may
     *            not be null
     */
    default <T> void forEachMatchingFieldName(String regex, int groupNumber, BiFunction<String, String, T> groupExtractor,
                                              BiConsumer<String, T> function) {
        forEachMatchingFieldName(Pattern.compile(regex), groupNumber, groupExtractor, function);
    }

    /**
     * For all fields whose names match the given regular expression, extract a value from the specified group in the regular
     * expression and call the supplied function.
     *
     * @param regex the regular expression string; may not be null
     * @param groupNumber the number of the regular expression group containing the string value to be extracted; must be positive
     * @param groupExtractor the function that extracts the value from the group
     * @param function the consumer that takes the value of matching field and the value extracted from the field name; may
     *            not be null
     */
    default <T> void forEachMatchingFieldName(Pattern regex, int groupNumber, BiFunction<String, String, T> groupExtractor,
                                              BiConsumer<String, T> function) {
        this.asMap().forEach((fieldName, fieldValue) -> {
            Matcher matcher = regex.matcher(fieldName);
            if (matcher.matches()) {
                String groupValue = matcher.group(groupNumber);
                T extractedValue = groupExtractor.apply(fieldName, groupValue);
                if (extractedValue != null) {
                    function.accept(fieldValue, extractedValue);
                }
            }
        });
    }

    /**
     * Call the supplied function for each of the fields.
     *
     * @param function the consumer that takes the field name and the string value extracted from the field; may
     *            not be null
     */
    default <T> void forEach(BiConsumer<String, String> function) {
        this.asMap().forEach(function);
    }
}
