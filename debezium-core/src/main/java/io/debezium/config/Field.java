/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.time.DateTimeException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import javax.lang.model.SourceVersion;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

import io.debezium.annotation.Immutable;
import io.debezium.function.Predicates;
import io.debezium.util.Strings;

/**
 * An immutable definition of a field that make appear within a {@link Configuration} instance.
 * @author Randall Hauch
 */
@Immutable
public final class Field {

    public static final String INTERNAL_PREFIX = "internal.";

    /**
     * Create a set of fields.
     * @param fields the fields to include
     * @return the field set; never null
     */
    public static Set setOf(Field... fields) {
        return new Set().with(fields);
    }

    /**
     * Create a set of fields.
     * @param fields the fields to include
     * @return the field set; never null
     */
    public static Set setOf(Iterable<Field> fields) {
        return new Set().with(fields);
    }

    /**
     * A set of fields.
     */
    @Immutable
    public static final class Set implements Iterable<Field> {
        private final Map<String, Field> fieldsByName;

        private Set() {
            this.fieldsByName = Collections.emptyMap();
        }

        private Set(Collection<Field> fields) {
            Map<String, Field> all = new LinkedHashMap<>();
            fields.forEach(field -> {
                if (field != null) {
                    all.put(field.name(), field);
                }
            });
            this.fieldsByName = Collections.unmodifiableMap(all);
        }

        /**
         * Get the field with the given {Field#name() name}.
         * @param name the name of the field
         * @return the field, or {@code null} if there is no field with the given name
         */
        public Field fieldWithName(String name) {
            return fieldsByName.get(name);
        }

        @Override
        public Iterator<Field> iterator() {
            return fieldsByName.values().iterator();
        }

        /**
         * Get the fields in this set as an array.
         * @return the array of fields; never null
         */
        public Field[] asArray() {
            return fieldsByName.values().toArray(new Field[0]);
        }

        /**
         * Call the supplied function for each of this set's fields that have non-existent dependents.
         * @param consumer the function; may not be null
         */
        public void forEachMissingDependent(Consumer<String> consumer) {
            fieldsByName.values().stream()
                    .map(Field::dependents)
                    .flatMap(Collection::stream)
                    .filter(Predicates.not(fieldsByName::containsKey))
                    .forEach(consumer);
        }

        /**
         * Call the supplied function for each of this set's fields that are not included as dependents in other
         * fields.
         * @param consumer the function; may not be null
         */
        public void forEachTopLevelField(Consumer<Field> consumer) {
            Collection<String> namesOfDependents = fieldsByName.values().stream()
                    .map(Field::dependents)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());
            fieldsByName.values().stream().filter(f -> !namesOfDependents.contains(f.name())).forEach(consumer);
        }

        /**
         * Get a new set that contains the fields in this set and those supplied.
         * @param fields the fields to include with this set's fields
         * @return the new set; never null
         */
        public Set with(Field... fields) {
            if (fields.length == 0) {
                return this;
            }
            LinkedHashSet<Field> all = new LinkedHashSet<>(this.fieldsByName.values());
            for (Field f : fields) {
                if (f != null) {
                    all.add(f);
                }
            }
            return new Set(all);
        }

        /**
         * Get a new set that contains the fields in this set and those supplied.
         * @param fields the fields to include with this set's fields
         * @return the new set; never null
         */
        public Set with(Iterable<Field> fields) {
            LinkedHashSet<Field> all = new LinkedHashSet<>(this.fieldsByName.values());
            fields.forEach(field -> {
                if (field != null) {
                    all.add(field);
                }
            });
            return new Set(all);
        }

        public java.util.Set<String> allFieldNames() {
            return this.fieldsByName.keySet();
        }

        public Set filtered(Predicate<Field> filter) {
            LinkedHashSet<Field> filtered = new LinkedHashSet<>();

            for (Entry<String, Field> field : fieldsByName.entrySet()) {
                if (filter.test(field.getValue())) {
                    filtered.add(field.getValue());
                }
            }

            return new Set(filtered);
        }
    }

    /**
     * A functional interface that accepts validation results.
     */
    @FunctionalInterface
    public interface ValidationOutput {
        /**
         * Accept a problem with the given value for the field.
         * @param field the field with the value; may not be null
         * @param value the value that is not valid
         * @param problemMessage the message describing the problem; may not be null
         */
        void accept(Field field, Object value, String problemMessage);
    }

    /**
     * A functional interface that can be used to validate field values.
     */
    @FunctionalInterface
    public interface Validator {

        /**
         * Validate the supplied value for the field, and report any problems to the designated consumer.
         *
         * @param config the configuration containing the field to be validated; may not be null
         * @param field the {@link Field} being validated; never null
         * @param problems the consumer to be called with each problem; never null
         * @return the number of problems that were found, or 0 if the value is valid
         */
        int validate(Configuration config, Field field, ValidationOutput problems);

        /**
         * Obtain a new {@link Validator} object that validates using this validator and the supplied validator.
         *
         * @param other the validation function to call after this
         * @return the new validator, or this validator if {@code other} is {@code null} or equal to {@code this}
         */
        default Validator and(Validator other) {
            if (other == null || other == this) {
                return this;
            }
            return (config, field, problems) -> validate(config, field, problems) + other.validate(config, field, problems);
        }
    }

    /**
     * A component that is able to provide recommended values for a field given a configuration.
     * In case that there are {@link Field#dependents() dependencies} between fields, the valid values and visibility
     * for a field may change given the values of other fields.
     */
    public interface Recommender {
        /**
         * Return a set of recommended (and valid) values for the field given the current configuration values.
         * @param field the field for which the recommended values are to be found; may not be null
         * @param config the configuration; may not be null
         * @return the list of valid values
         */
        List<Object> validValues(Field field, Configuration config);

        /**
         * Set the visibility of the field given the current configuration values.
         * @param field the field; may not be null
         * @param config the configuration; may not be null
         * @return {@code true} if the field is to be visible, or {@code false} otherwise
         */
        boolean visible(Field field, Configuration config);
    }

    public enum Group {
        CONNECTION,
        CONNECTION_ADVANCED_SSL,
        CONNECTION_ADVANCED,
        CONNECTION_ADVANCED_REPLICATION,
        CONNECTION_ADVANCED_PUBLICATION,
        FILTERS,
        CONNECTOR_SNAPSHOT,
        CONNECTOR,
        ADVANCED_HEARTBEAT,
        CONNECTOR_ADVANCED,
        ADVANCED
    };

    public static class GroupEntry {
        private final Group group;
        private final int positionInGroup;

        GroupEntry(Group group, int positionInGroup) {
            this.group = group;
            this.positionInGroup = positionInGroup;
        }

        public Group getGroup() {
            return group;
        }

        public int getPositionInGroup() {
            return positionInGroup;
        }
    }

    public static GroupEntry createGroupEntry(Group group) {
        return new GroupEntry(group, 9999);
    }

    public static GroupEntry createGroupEntry(Group group, int positionInGroup) {
        return new GroupEntry(group, positionInGroup);
    }

    /**
     * Create an immutable {@link Field} instance with the given property name.
     * @param name the name of the field; may not be null
     * @return the field; never null
     */
    public static Field create(String name) {
        return new Field(name, null, null, null, null, null, null, null);
    }

    /**
     * Create an immutable internal {@link Field} instance with the given property name.
     * The name will be prefixed with {@code internal.} prefix.
     *
     * @param name the name of the field; may not be null
     * @return the field; never null
     */
    public static Field createInternal(String name) {
        return new Field(INTERNAL_PREFIX + name, null, null, null, null, null, null, null, null, null);
    }

    /**
     * Create an immutable {@link Field} instance with the given property name.
     * @param name the name of the field; may not be null
     * @param displayName the display name of the field; may not be null
     * @return the field; never null
     */
    public static Field create(String name, String displayName) {
        return new Field(name, displayName, null, null, null, null, null, null);
    }

    /**
     * Create an immutable {@link Field} instance with the given property name and description.
     * @param name the name of the field; may not be null
     * @param displayName the display name of the field; may not be null
     * @param description the description
     * @return the field; never null
     */
    public static Field create(String name, String displayName, String description) {
        return new Field(name, displayName, null, null, description, null, null, null);
    }

    /**
     * Create an immutable {@link Field} instance with the given property name, description, and default value.
     * @param name the name of the field; may not be null
     * @param displayName the display name of the field; may not be null
     * @param description the description
     * @param defaultValue the default value for the field
     * @return the field; never null
     */
    public static Field create(String name, String displayName, String description, String defaultValue) {
        return new Field(name, displayName, Type.STRING, null, description, null, () -> defaultValue, null);
    }

    /**
     * Create an immutable {@link Field} instance with the given property name, description, and default value.
     * @param name the name of the field; may not be null
     * @param displayName the display name of the field; may not be null
     * @param description the description
     * @param defaultValue the default value for the field
     * @return the field; never null
     */
    public static Field create(String name, String displayName, String description, int defaultValue) {
        return new Field(name, displayName, Type.INT, null, description, null, () -> Integer.toString(defaultValue), null);
    }

    /**
     * Create an immutable {@link Field} instance with the given property name, description, and default value.
     * @param name the name of the field; may not be null
     * @param displayName the display name of the field; may not be null
     * @param description the description
     * @param defaultValue the default value for the field
     * @return the field; never null
     */
    public static Field create(String name, String displayName, String description, long defaultValue) {
        return new Field(name, displayName, Type.LONG, null, description, null, () -> Long.toString(defaultValue), null);
    }

    /**
     * Create an immutable {@link Field} instance with the given property name, description, and default value.
     * @param name the name of the field; may not be null
     * @param displayName the display name of the field; may not be null
     * @param description the description
     * @param defaultValue the default value for the field
     * @return the field; never null
     */
    public static Field create(String name, String displayName, String description, boolean defaultValue) {
        return new Field(name, displayName, Type.BOOLEAN, null, description, null, () -> Boolean.toString(defaultValue), null);
    }

    /**
     * Create an immutable {@link Field} instance with the given property name, description, and default value.
     * @param name the name of the field; may not be null
     * @param displayName the display name of the field; may not be null
     * @param description the description
     * @param defaultValueGenerator the generator for the default value for the field
     * @return the field; never null
     */
    public static Field create(String name, String displayName, String description, Supplier<Object> defaultValueGenerator) {
        return new Field(name, displayName, Type.STRING, null, description, null, defaultValueGenerator, null);
    }

    /**
     * Create an immutable {@link Field} instance with the given property name, description, and default value.
     *
     * @param name the name of the field; may not be null
     * @param displayName the display name of the field; may not be null
     * @param description the description
     * @param defaultValueGenerator the generator for the default value for the field
     * @return the field; never null
     */
    public static Field create(String name, String displayName, String description, BooleanSupplier defaultValueGenerator) {
        return new Field(name, displayName, Type.BOOLEAN, null, description, null,
                defaultValueGenerator::getAsBoolean, null);
    }

    /**
     * Create an immutable {@link Field} instance with the given property name, description, and default value.
     * @param name the name of the field; may not be null
     * @param displayName the display name of the field; may not be null
     * @param description the description
     * @param defaultValueGenerator the generator for the default value for the field
     * @return the field; never null
     */
    public static Field create(String name, String displayName, String description, IntSupplier defaultValueGenerator) {
        return new Field(name, displayName, Type.INT, null, description, null,
                defaultValueGenerator::getAsInt, null);
    }

    /**
     * Create an immutable {@link Field} instance with the given property name, description, and default value.
     * @param name the name of the field; may not be null
     * @param displayName the display name of the field; may not be null
     * @param description the description
     * @param defaultValueGenerator the generator for the default value for the field
     * @return the field; never null
     */
    public static Field create(String name, String displayName, String description, LongSupplier defaultValueGenerator) {
        return new Field(name, displayName, Type.LONG, null, description, null,
                defaultValueGenerator::getAsLong, null);
    }

    /**
     * Add this field to the given configuration definition.
     * @param configDef the definition of the configuration; may be null if none of the fields are to be added
     * @param groupName the name of the group; may be null
     * @param fields the fields to be added as a group to the definition of the configuration
     * @return the updated configuration; never null
     */
    public static ConfigDef group(ConfigDef configDef, String groupName, Field... fields) {
        if (configDef != null) {
            if (groupName != null) {
                for (int i = 0; i != fields.length; ++i) {
                    Field f = fields[i];
                    configDef.define(f.name(), f.type(), f.defaultValue(), null, f.importance(), f.description(),
                            groupName, i + 1, f.width(), f.displayName(), f.dependents(), null);
                }
            }
            else {
                for (int i = 0; i != fields.length; ++i) {
                    Field f = fields[i];
                    configDef.define(f.name(), f.type(), f.defaultValue(), null, f.importance(), f.description(),
                            null, 1, f.width(), f.displayName(), f.dependents(), null);
                }
            }
        }
        return configDef;
    }

    private final String name;
    private final String displayName;
    private final String desc;
    private final Supplier<Object> defaultValueGenerator;
    private final Validator validator;
    private final Width width;
    private final Type type;
    private final Importance importance;
    private final List<String> dependents;
    private final Recommender recommender;
    private final java.util.Set<?> allowedValues;
    private final GroupEntry group;
    private final boolean isRequired;

    protected Field(String name, String displayName, Type type, Width width, String description, Importance importance,
                    Supplier<Object> defaultValueGenerator, Validator validator) {
        this(name, displayName, type, width, description, importance, null, defaultValueGenerator, validator, null);
    }

    protected Field(String name, String displayName, Type type, Width width, String description, Importance importance,
                    List<String> dependents, Supplier<Object> defaultValueGenerator, Validator validator,
                    Recommender recommender) {
        this(name, displayName, type, width, description, importance, dependents, defaultValueGenerator, validator,
                recommender, false, Field.createGroupEntry(Group.ADVANCED), Collections.emptySet());
    }

    protected Field(String name, String displayName, Type type, Width width, String description, Importance importance,
                    List<String> dependents, Supplier<Object> defaultValueGenerator, Validator validator,
                    Recommender recommender, boolean isRequired, GroupEntry group, java.util.Set<?> allowedValues) {
        Objects.requireNonNull(name, "The field name is required");
        this.name = name;
        this.displayName = displayName;
        this.desc = description;
        this.defaultValueGenerator = defaultValueGenerator != null ? defaultValueGenerator : () -> null;
        this.validator = validator;
        this.type = type != null ? type : Type.STRING;
        this.width = width != null ? width : Width.NONE;
        this.importance = importance != null ? importance : Importance.MEDIUM;
        this.dependents = dependents != null ? dependents : Collections.emptyList();
        this.recommender = recommender;
        this.isRequired = isRequired;
        this.group = group;
        this.allowedValues = allowedValues;
        assert this.name != null;
    }

    /**
     * Get the name of the field.
     * @return the name; never null
     */
    public String name() {
        return name;
    }

    /**
     * Get the default value of the field.
     * @return the default value, or {@code null} if there is no default value
     */
    public Object defaultValue() {
        return defaultValueGenerator.get();
    }

    /**
     * Get the string representation of the default value of the field.
     * @return the default value, or {@code null} if there is no default value
     */
    public String defaultValueAsString() {
        Object defaultValue = defaultValue();
        return defaultValue != null ? defaultValue.toString() : null;
    }

    /**
     * Get the description of the field.
     * @return the description; never null
     */
    public String description() {
        return desc;
    }

    /**
     * Get the display name of the field.
     * @return the display name; never null
     */
    public String displayName() {
        return displayName;
    }

    /**
     * Get the width of this field.
     * @return the width; never null
     */
    public Width width() {
        return width;
    }

    /**
     * Get the type of this field.
     * @return the type; never null
     */
    public Type type() {
        return type;
    }

    /**
     * Get the importance of this field.
     * @return the importance; never null
     */
    public Importance importance() {
        return importance;
    }

    /**
     * Get the names of the fields that are or may be dependent upon this field.
     * @return the list of dependents; never null but possibly empty
     */
    public List<String> dependents() {
        return dependents;
    }

    /**
     * Get the validator for this field.
     * @return the validator; may be null if there is no validator
     */
    public Validator validator() {
        return validator;
    }

    /**
     * Get the {@link Recommender} for this field.
     * @return the recommender; may be null if there is no recommender
     */
    public Recommender recommender() {
        return recommender;
    }

    /**
     * Get if the field is required/mandatory.
     * @return if the field is required; true or false
     */
    public boolean isRequired() {
        return isRequired;
    }

    /**
     * Get the group of this field.
     * @return the group
     */
    public GroupEntry group() {
        return group;
    }

    /**
     * Get the allowed values for this field.
     * @return the java.util.Set of allowed values; may be null if there's no set of specific values
     */
    public java.util.Set<?> allowedValues() {
        return allowedValues;
    }

    /**
     * Validate the supplied value for this field, and report any problems to the designated consumer.
     * @param config the field values keyed by their name; may not be null
     * @param problems the consumer to be called with each problem; never null
     * @return {@code true} if the value is considered valid, or {@code false} if it is not valid
     */
    public boolean validate(Configuration config, ValidationOutput problems) {
        Validator typeValidator = validatorForType(type);
        int errors = 0;
        if (typeValidator != null) {
            errors += typeValidator.validate(config, this, problems);
        }
        if (validator != null) {
            errors += validator.validate(config, this, problems);
        }
        return errors == 0;
    }

    /**
     * Validate this field in the supplied configuration, updating the {@link ConfigValue} for the field with the results.
     * @param config the configuration to be validated; may not be null
     * @param fieldSupplier the supplier for dependent fields by name; may not be null
     * @param results the set of configuration results keyed by field name; may not be null
     */
    protected void validate(Configuration config, Function<String, Field> fieldSupplier, Map<String, ConfigValue> results) {
        // First, merge any new recommended values ...
        ConfigValue value = results.computeIfAbsent(this.name(), ConfigValue::new);

        // Apply the validator ...
        validate(config, (f, v, problem) -> value.addErrorMessage(validationOutput(f, problem)));

        // Apply the recommender ..
        if (recommender != null) {
            try {
                // Set the visibility ...
                value.visible(recommender.visible(this, config));
                // Compute and set the new recommendations ...
                List<Object> newRecommendations = recommender.validValues(this, config);
                List<Object> previousRecommendations = value.recommendedValues();
                if (!previousRecommendations.isEmpty()) {
                    // Don't use any newly recommended values if they were not previously recommended ...
                    newRecommendations.retainAll(previousRecommendations);
                }
                value.recommendedValues(newRecommendations);
            }
            catch (ConfigException e) {
                value.addErrorMessage(e.getMessage());
            }
        }

        // Do the same for any dependents ...
        dependents.forEach(name -> {
            Field dependentField = fieldSupplier.apply(name);
            if (dependentField != null) {
                dependentField.validate(config, fieldSupplier, results);
            }
        });
    }

    /**
     * Create and return a new Field instance that is a copy of this field but with the given description.
     * @param description the new description for the new field
     * @return the new field; never null
     */
    public Field withDescription(String description) {
        return new Field(name(), displayName, type(), width, description, importance(), dependents,
                defaultValueGenerator, validator, recommender, isRequired, group, allowedValues);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but with the given display name.
     *
     * @param displayName the new display name for the field
     * @return the new field; never null
     */
    public Field withDisplayName(String displayName) {
        return new Field(name(), displayName, type(), width, description(), importance(), dependents,
                defaultValueGenerator, validator, recommender, isRequired, group, allowedValues);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but with the given width.
     * @param width the new width for the field
     * @return the new field; never null
     */
    public Field withWidth(Width width) {
        return new Field(name(), displayName(), type(), width, description(), importance(), dependents,
                defaultValueGenerator, validator, recommender, isRequired, group, allowedValues);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but with the given type.
     * @param type the new type for the field
     * @return the new field; never null
     */
    public Field withType(Type type) {
        return new Field(name(), displayName(), type, width(), description(), importance(), dependents,
                defaultValueGenerator, validator, recommender, isRequired, group, allowedValues);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but has a {@link #withType(Type) type} of
     * {@link org.apache.kafka.connect.data.Schema.Type#STRING}, a {@link #withRecommender(Recommender) recommender}
     * that returns a list of {@link Enum#name() Enum names} as valid values, and a validator that verifies values are valid
     * enumeration names.
     * @param enumType the enumeration type for the field
     * @return the new field; never null
     */
    public <T extends Enum<T>> Field withEnum(Class<T> enumType) {
        return withEnum(enumType, null);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but has a {@link #withType(Type) type} of
     * {@link org.apache.kafka.connect.data.Schema.Type#STRING}, a {@link #withRecommender(Recommender) recommender}
     * that returns a list of {@link Enum#name() Enum names} as valid values, and a validator that verifies values are valid
     * enumeration names.
     * @param enumType the enumeration type for the field
     * @param defaultOption the default enumeration value; may be null
     * @return the new field; never null
     */
    public <T extends Enum<T>> Field withEnum(Class<T> enumType, T defaultOption) {
        EnumRecommender<T> recommendator = new EnumRecommender<>(enumType);
        Field result = withType(Type.STRING).withRecommender(recommendator).withValidation(recommendator)
                .withAllowedValues(getEnumLiterals(enumType));
        // Not all enums support EnumeratedValue yet
        if (defaultOption != null) {
            if (defaultOption instanceof EnumeratedValue) {
                result = result.withDefault(((EnumeratedValue) defaultOption).getValue());
            }
            else {
                result = result.withDefault(defaultOption.name().toLowerCase());
            }
        }
        return result;
    }

    public Field required() {
        return new Field(name(), displayName(), type(), width(), description(), importance, dependents,
                defaultValueGenerator, validator, recommender, true, group, allowedValues)
                .withValidation(Field::isRequired);
    }

    public Field optional() {
        return new Field(name(), displayName(), type(), width(), description(), importance, dependents,
                defaultValueGenerator, validator, recommender, false, group, allowedValues);
    }

    public Field withGroup(GroupEntry group) {
        return new Field(name(), displayName(), type(), width(), description(), importance, dependents,
                defaultValueGenerator, validator, recommender, isRequired, group, allowedValues);
    }

    public Field withAllowedValues(java.util.Set<?> allowedValues) {
        return new Field(name(), displayName(), type(), width(), description(), importance, dependents,
                defaultValueGenerator, validator, recommender, isRequired, group, allowedValues);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but with the given importance.
     * @param importance the new importance for the field
     * @return the new field; never null
     */
    public Field withImportance(Importance importance) {
        return new Field(name(), displayName(), type(), width(), description(), importance, dependents,
                defaultValueGenerator, validator, recommender, isRequired, group, allowedValues);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but with the given display name.
     * @param dependents the names of the fields that depend on this field
     * @return the new field; never null
     */
    public Field withDependents(String... dependents) {
        return new Field(name(), displayName(), type(), width, description(), importance(),
                Arrays.asList(dependents), defaultValueGenerator, validator, recommender, isRequired, group, allowedValues);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but with the given default value.
     * @param defaultValue the new default value for the new field
     * @return the new field; never null
     */
    public Field withDefault(String defaultValue) {
        return new Field(name(), displayName(), type(), width, description(), importance(), dependents,
                () -> defaultValue, validator, recommender, isRequired, group, allowedValues);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but with the given default value.
     *
     * @param defaultValue the new default value for the new field
     * @return the new field; never null
     */
    public Field withDefault(boolean defaultValue) {
        return new Field(name(), displayName(), type(), width, description(), importance(), dependents,
                () -> Boolean.valueOf(defaultValue), validator, recommender, isRequired, group, allowedValues);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but with the given default value.
     * @param defaultValue the new default value for the new field
     * @return the new field; never null
     */
    public Field withDefault(int defaultValue) {
        return new Field(name(), displayName(), type(), width, description(), importance(), dependents,
                () -> defaultValue, validator, recommender, isRequired, group, allowedValues);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but with the given default value.
     *
     * @param defaultValue the new default value for the new field
     * @return the new field; never null
     */
    public Field withDefault(long defaultValue) {
        return new Field(name(), displayName(), type(), width, description(), importance(), dependents,
                () -> defaultValue, validator, recommender, isRequired, group, allowedValues);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but with the given default value.
     *
     * @param defaultValueGenerator the supplier for the new default value for the new field, called whenever a default value
     *            is needed
     * @return the new field; never null
     */
    public Field withDefault(BooleanSupplier defaultValueGenerator) {
        return new Field(name(), displayName(), type(), width, description(), importance(), dependents,
                defaultValueGenerator::getAsBoolean, validator, recommender, isRequired, group, allowedValues);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but with the given default value.
     *
     * @param defaultValueGenerator the supplier for the new default value for the new field, called whenever a default value
     *            is needed
     * @return the new field; never null
     */
    public Field withDefault(IntSupplier defaultValueGenerator) {
        return new Field(name(), displayName(), type(), width, description(), importance(), dependents,
                defaultValueGenerator::getAsInt, validator, recommender, isRequired, group, allowedValues);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but with the given default value.
     *
     * @param defaultValueGenerator the supplier for the new default value for the new field, called whenever a default value
     *            is needed
     * @return the new field; never null
     */
    public Field withDefault(LongSupplier defaultValueGenerator) {
        return new Field(name(), displayName(), type(), width, description(), importance(), dependents,
                defaultValueGenerator::getAsLong, validator, recommender, isRequired, group, allowedValues);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but with the given recommender.
     *
     * @param recommender the recommender; may be null
     * @return the new field; never null
     */
    public Field withRecommender(Recommender recommender) {
        return new Field(name(), displayName(), type(), width, description(), importance(), dependents,
                defaultValueGenerator, validator, recommender, isRequired, group, allowedValues);
    }

    public Field withInvisibleRecommender() {
        return withRecommender(new InvisibleRecommender());
    }

    /**
     * Create and return a new Field instance that is a copy of this field but that uses no validation.
     *
     * @return the new field; never null
     */
    public Field withNoValidation() {
        return new Field(name(), displayName(), type(), width, description(), importance(), dependents,
                defaultValueGenerator, null, recommender, isRequired, group, allowedValues);
    }

    /**
     * Create and return a new Field instance that is a copy of this field but that in addition to {@link #validator() existing
     * validation} the supplied validation function(s) are also used.
     *
     * @param validators the additional validation function(s); may be null
     * @return the new field; never null
     */
    public Field withValidation(Validator... validators) {
        Validator actualValidator = validator;
        for (Validator validator : validators) {
            if (validator != null) {
                actualValidator = validator.and(actualValidator);
            }
        }
        return new Field(name(), displayName(), type(), width(), description(), importance(), dependents,
                defaultValueGenerator, actualValidator, recommender, isRequired, group, allowedValues);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof Field) {
            Field that = (Field) obj;
            return this.name().equals(that.name());
        }
        return false;
    }

    @Override
    public String toString() {
        return name();
    }

    /**
     * Validation logic for numeric ranges
     */
    public static class RangeValidator implements Validator {
        private final Number min;
        private final Number max;

        private RangeValidator(Number min, Number max) {
            this.min = min;
            this.max = max;
        }

        /**
         * A validator that checks only the lower numerical bound.
         *
         * @param min the minimum acceptable value; may not be null
         * @return the validator; never null
         */
        public static RangeValidator atLeast(Number min) {
            return new RangeValidator(min, null);
        }

        /**
         * A validator that checks both the upper and lower bound.
         *
         * @param min the minimum acceptable value; may not be null
         * @param max the maximum acceptable value; may not be null
         * @return the validator; never null
         */
        public static RangeValidator between(Number min, Number max) {
            return new RangeValidator(min, max);
        }

        @Override
        public int validate(Configuration config, Field field, ValidationOutput problems) {
            Number value = config.getNumber(field);
            if (value == null) {
                problems.accept(field, value, "A value must be provided");
                return 1;
            }
            if (min != null && value.doubleValue() < min.doubleValue()) {
                problems.accept(field, value, "Value must be at least " + min);
                return 1;
            }
            if (max != null && value.doubleValue() > max.doubleValue()) {
                problems.accept(field, value, "Value must be no more than " + max);
                return 1;
            }
            return 0;
        }

        public void ensureValid(String name, Object o) {
            if (o == null) {
                throw new ConfigException(name, o, "Value must be non-null");
            }
            Number n = (Number) o;
            if (min != null && n.doubleValue() < min.doubleValue()) {
                throw new ConfigException(name, o, "Value must be at least " + min);
            }
            if (max != null && n.doubleValue() > max.doubleValue()) {
                throw new ConfigException(name, o, "Value must be no more than " + max);
            }
        }

        @Override
        public String toString() {
            if (min == null) {
                return "[...," + max + "]";
            }
            else {
                if (max == null) {
                    return "[" + min + ",...]";
                }
                else {
                    return "[" + min + ",...," + max + "]";
                }
            }
        }
    }

    /**
     * A {@link Recommender} that will look at several fields that are deemed to be exclusive, such that when the first of
     * them has a value the others are made invisible.
     */
    public static class OneOfRecommender implements Recommender {

        protected final List<String> possibleNames;

        public OneOfRecommender(String... possibleNames) {
            this(Arrays.asList(possibleNames));
        }

        public OneOfRecommender(List<String> possibleNames) {
            this.possibleNames = possibleNames;
        }

        @Override
        public List<Object> validValues(Field field, Configuration config) {
            return new LinkedList<>();
        }

        @Override
        public boolean visible(Field field, Configuration config) {
            for (String possibleName : possibleNames) {
                Object value = config.getString(possibleName);
                if (value != null) {
                    // There is a value for this possible name, the name is visible if it has a value ...
                    return possibleName.equals(field.name());
                }
            }
            // There are no values for any of the possibles, so they all are visible ...
            return true;
        }
    }

    private static <T extends Enum<T>> java.util.Set<String> getEnumLiterals(Class<T> enumType) {
        if (Arrays.asList(enumType.getInterfaces()).contains(EnumeratedValue.class)) {
            return Arrays.stream(enumType.getEnumConstants())
                    .map(x -> ((EnumeratedValue) x).getValue())
                    .map(String::toLowerCase)
                    .collect(Collectors.toSet());
        }
        else {
            return Arrays.stream(enumType.getEnumConstants())
                    .map(Enum::name)
                    .map(String::toLowerCase)
                    .collect(Collectors.toSet());
        }
    }

    public static class EnumRecommender<T extends Enum<T>> implements Recommender, Validator {

        private final List<Object> validValues;
        private final java.util.Set<String> literals;
        private final String literalsStr;

        public EnumRecommender(Class<T> enumType) {
            // Not all enums support EnumeratedValue yet
            this.literals = getEnumLiterals(enumType);
            this.validValues = Collections.unmodifiableList(new ArrayList<>(this.literals));
            this.literalsStr = Strings.join(", ", validValues);
        }

        @Override
        public List<Object> validValues(Field field, Configuration config) {
            return validValues;
        }

        @Override
        public boolean visible(Field field, Configuration config) {
            return true;
        }

        @Override
        public int validate(Configuration config, Field field, ValidationOutput problems) {
            String value = config.getString(field);
            if (value == null) {
                problems.accept(field, value, "Value must be one of " + literalsStr);
                return 1;
            }
            String trimmed = value.trim().toLowerCase();
            if (!literals.contains(trimmed)) {
                problems.accept(field, value, "Value must be one of " + literalsStr);
                return 1;
            }
            return 0;
        }
    }

    /**
     * A {@link Recommender} that will look at several fields that are deemed to be exclusive, such that when the first of
     * them has a value the others are made invisible.
     */
    public static class InvisibleRecommender implements Recommender {

        public InvisibleRecommender() {
        }

        @Override
        public List<Object> validValues(Field field, Configuration config) {
            return new LinkedList<>();
        }

        @Override
        public boolean visible(Field field, Configuration config) {
            return false;
        }
    }

    public static Validator validatorForType(Type type) {
        switch (type) {
            case BOOLEAN:
                return Field::isBoolean;
            case CLASS:
                return Field::isClassName;
            case DOUBLE:
                return Field::isDouble;
            case INT:
                return Field::isInteger;
            case SHORT:
                return Field::isShort;
            case LONG:
                return Field::isLong;
            case STRING:
            case LIST:
            case PASSWORD:
                break;
        }
        return null;
    }

    public static int isListOfRegex(Configuration config, Field field, ValidationOutput problems) {
        String value = config.getString(field);
        int errors = 0;
        if (value != null) {
            try {
                Strings.setOfRegex(value, Pattern.CASE_INSENSITIVE);
            }
            catch (PatternSyntaxException e) {
                problems.accept(field, value, "A comma-separated list of valid regular expressions is expected, but " + e.getMessage());
                ++errors;
            }
        }
        return errors;
    }

    public static int isRegex(Configuration config, Field field, ValidationOutput problems) {
        String value = config.getString(field);
        int errors = 0;
        if (value != null) {
            try {
                Pattern.compile(value, Pattern.CASE_INSENSITIVE);
            }
            catch (PatternSyntaxException e) {
                problems.accept(field, value, "A valid regular expressions is expected, but " + e.getMessage());
                ++errors;
            }
        }
        return errors;
    }

    public static int isClassName(Configuration config, Field field, ValidationOutput problems) {
        String value = config.getString(field);
        if (value == null || SourceVersion.isName(value)) {
            return 0;
        }
        problems.accept(field, value, "A Java class name is expected");
        return 1;
    }

    public static int isRequired(Configuration config, Field field, ValidationOutput problems) {
        String value = config.getString(field);
        if (value != null && value.trim().length() > 0) {
            return 0;
        }
        problems.accept(field, value, "A value is required");
        return 1;
    }

    public static int isOptional(Configuration config, Field field, ValidationOutput problems) {
        // optional fields are valid whether or not there is a value
        return 0;
    }

    public static int isBoolean(Configuration config, Field field, ValidationOutput problems) {
        String value = config.getString(field);
        if (value == null ||
                value.trim().equalsIgnoreCase(Boolean.TRUE.toString()) ||
                value.trim().equalsIgnoreCase(Boolean.FALSE.toString())) {
            return 0;
        }
        problems.accept(field, value, "Either 'true' or 'false' is expected");
        return 1;
    }

    public static int isInteger(Configuration config, Field field, ValidationOutput problems) {
        String value = config.getString(field);
        if (value == null) {
            return 0;
        }
        try {
            Integer.parseInt(value);
        }
        catch (NumberFormatException e) {
            problems.accept(field, value, "An integer is expected");
            return 1;
        }
        return 0;
    }

    public static int isPositiveInteger(Configuration config, Field field, ValidationOutput problems) {
        String value = config.getString(field);
        if (value == null) {
            return 0;
        }
        try {
            if (Integer.parseInt(value) > 0) {
                return 0;
            }
        }
        catch (Throwable e) {
        }
        problems.accept(field, value, "A positive, non-zero integer value is expected");
        return 1;
    }

    public static int isNonNegativeInteger(Configuration config, Field field, ValidationOutput problems) {
        String value = config.getString(field);
        if (value == null) {
            return 0;
        }
        try {
            if (Integer.parseInt(value) >= 0) {
                return 0;
            }
        }
        catch (Throwable e) {
        }
        problems.accept(field, value, "An non-negative integer is expected");
        return 1;
    }

    public static int isLong(Configuration config, Field field, ValidationOutput problems) {
        String value = config.getString(field);
        if (value == null) {
            return 0;
        }
        try {
            Long.parseLong(value);
        }
        catch (NumberFormatException e) {
            problems.accept(field, value, "A long value is expected");
            return 1;
        }
        return 0;
    }

    public static int isPositiveLong(Configuration config, Field field, ValidationOutput problems) {
        String value = config.getString(field);
        if (value == null) {
            return 0;
        }
        try {
            if (Long.parseLong(value) > 0) {
                return 0;
            }
        }
        catch (Throwable e) {
        }
        problems.accept(field, value, "A positive, non-zero long value is expected");
        return 1;
    }

    public static int isNonNegativeLong(Configuration config, Field field, ValidationOutput problems) {
        String value = config.getString(field);
        if (value == null) {
            return 0;
        }
        try {
            if (Long.parseLong(value) >= 0) {
                return 0;
            }
        }
        catch (Throwable e) {
        }
        problems.accept(field, value, "A non-negative long value is expected");
        return 1;
    }

    public static int isShort(Configuration config, Field field, ValidationOutput problems) {
        String value = config.getString(field);
        if (value == null) {
            return 0;
        }
        try {
            Short.parseShort(value);
        }
        catch (NumberFormatException e) {
            problems.accept(field, value, "A short value is expected");
            return 1;
        }
        return 0;
    }

    public static int isDouble(Configuration config, Field field, ValidationOutput problems) {
        String value = config.getString(field);
        if (value == null) {
            return 0;
        }
        try {
            Double.parseDouble(value);
        }
        catch (NumberFormatException e) {
            problems.accept(field, value, "A double value is expected");
            return 1;
        }
        return 0;
    }

    public static int isZoneOffset(Configuration config, Field field, ValidationOutput problems) {
        String value = config.getString(field);
        if (value == null) {
            return 0;
        }
        try {
            ZoneOffset.of(value);
        }
        catch (DateTimeException e) {
            problems.accept(field, value, "A zone offset string representation is expected");
            return 1;
        }
        return 0;
    }

    public static String validationOutput(Field field, String problem) {
        return String.format("The '%s' value is invalid: %s", field.name(), problem);
    }
}
