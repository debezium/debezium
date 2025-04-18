/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.charset;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.connector.binlog.charset.BinlogCharsetRegistry;

/**
 * A registry that stores character set mappings from {@code charset_mappings.json} for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbCharsetRegistry implements BinlogCharsetRegistry {

    private static final String CHARSET_MAPPINGS = "charset_mappings.json";
    private static final int MAP_SIZE = 1024;

    private final List<String> collationIndexToCollationName = new ArrayList<>(Collections.nCopies(MAP_SIZE, null));
    private final Map<Integer, CharacterSetMapping> collationIndexToCharacterSet = new TreeMap<>();
    private final Map<String, CharacterSetMapping> characterSetNameToCharacterSet = new HashMap<>();

    public MariaDbCharsetRegistry() {
        loadCharacterSetMappingsFromFileResource();
    }

    @Override
    public int getCharsetMapSize() {
        return MAP_SIZE;
    }

    @Override
    public String getCollationNameForCollationIndex(Integer collationIndex) {
        String newValue = null;
        if (!Objects.isNull(collationIndex) && isWithinRange(collationIndex)) {
            newValue = collationIndexToCollationName.get(collationIndex);
        }
        return newValue;
    }

    @Override
    public String getCharsetNameForCollationIndex(Integer collationIndex) {
        String newValue = null;
        if (!Objects.isNull(collationIndex)) {
            CharacterSetMapping mapping = collationIndexToCharacterSet.get(collationIndex);
            if (!Objects.isNull(mapping)) {
                newValue = mapping.name;
            }
        }
        return newValue;
    }

    @Override
    public String getJavaEncodingForCharSet(String characterSetName) {
        final CharacterSetMapping mapping = characterSetNameToCharacterSet.get(characterSetName);
        if (!Objects.isNull(mapping)) {
            return mapping.getFirstEncoding();
        }
        return null;
    }

    private void loadCharacterSetMappingsFromFileResource() {
        try (InputStream stream = MariaDbCharsetRegistry.class.getClassLoader().getResourceAsStream(CHARSET_MAPPINGS)) {
            final ObjectMapper mapper = new ObjectMapper();
            final CharacterSetMappings mappings = mapper.readValue(stream, CharacterSetMappings.class);
            loadCharacterSetMappings(mappings.characterSets);
            loadCollationMappings(mappings.collations);
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to load character set mappings", e);
        }
    }

    private void loadCharacterSetMappings(List<CharacterSetMapping> characterSetMappings) {
        for (CharacterSetMapping charsetMapping : characterSetMappings) {
            final String characterSetName = charsetMapping.name;
            characterSetNameToCharacterSet.put(characterSetName, charsetMapping);
            if (!Objects.isNull(charsetMapping.aliases)) {
                for (String alias : charsetMapping.aliases) {
                    characterSetNameToCharacterSet.put(alias, charsetMapping);
                }
            }
        }
    }

    private void loadCollationMappings(List<CollationMapping> collationMappings) {
        for (CollationMapping collation : collationMappings) {
            final CharacterSetMapping mapping = characterSetNameToCharacterSet.get(collation.charSetName);
            collationIndexToCollationName.set(collation.index, collation.collations.get(0));
            collationIndexToCharacterSet.put(collation.index, mapping);
        }
    }

    private static boolean isWithinRange(int value) {
        return value > 0 && value < MAP_SIZE;
    }

    /**
     * Represents the data within the file {@code charset_mappings.json}.
     */
    private static class CharacterSetMappings {
        List<CharacterSetMapping> characterSets;
        List<CollationMapping> collations;

        @JsonCreator
        CharacterSetMappings(@JsonProperty("character_sets") List<CharacterSetMapping> characterSets,
                             @JsonProperty("collation_mappings") List<CollationMapping> collationMappings) {
            this.characterSets = characterSets;
            this.collations = collationMappings;
        }
    }

    /**
     * Represents a character set mapping
     */
    private static class CharacterSetMapping {
        private static final String UTF8 = "UTF-8";
        private static final String CP1252 = "Cp1252";

        String name;
        int multiByteLength;
        int priority;
        List<String> encodings;
        List<String> aliases;
        String comment;

        @JsonCreator
        CharacterSetMapping(@JsonProperty("name") String name,
                            @JsonProperty("mblen") int multiByteLength,
                            @JsonProperty("priority") int priority,
                            @JsonProperty("encodings") List<String> encodings,
                            @JsonProperty("aliases") List<String> aliases,
                            @JsonProperty("comment") String comment) {
            this.name = name;
            this.multiByteLength = multiByteLength;
            this.priority = priority;
            this.encodings = new ArrayList<>();
            this.aliases = Objects.isNull(aliases) ? new ArrayList<>() : aliases;
            this.comment = comment;

            addEncodings(encodings);
        }

        private String getFirstEncoding() {
            return encodings.get(0);
        }

        private void addEncodings(List<String> encodings) {
            for (String encoding : encodings) {
                try {
                    Charset charset = Charset.forName(encoding);
                    addEncodingMapping(charset.name());
                    charset.aliases().forEach(this::addEncodingMapping);
                }
                catch (Exception e) {
                    if (multiByteLength == 1) {
                        addEncodingMapping(encoding);
                    }
                }
            }
            if (this.encodings.isEmpty()) {
                addEncodingMapping(multiByteLength > 1 ? UTF8 : CP1252);
            }
        }

        private void addEncodingMapping(String encoding) {
            final String encodingValue = encoding.toUpperCase(Locale.ENGLISH);
            if (!encodings.contains(encodingValue)) {
                encodings.add(encodingValue);
            }
        }
    }

    /**
     * Represents a collation mapping to a set of database character sets.
     */
    private static class CollationMapping {
        int index;
        List<String> collations;
        int priority;
        String charSetName;

        @JsonCreator
        CollationMapping(@JsonProperty("index") int index,
                         @JsonProperty("collations") List<String> collations,
                         @JsonProperty("priority") int priority,
                         @JsonProperty("charset") String charSetName) {
            this.index = index;
            this.collations = collations;
            this.priority = priority;
            this.charSetName = charSetName;
        }
    }
}
