/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigException;
import org.bson.BsonDocument;
import org.bson.Document;

import io.debezium.annotation.ThreadSafe;
import io.debezium.util.Strings;

/**
 * This filter selector is designed to determine the filter to exclude or rename fields in a document.
 */
@ThreadSafe
public final class FieldSelector {

    private static final Pattern DOT = Pattern.compile("\\.");
    private static final Pattern COLON = Pattern.compile(":");

    /**
     * This filter is designed to exclude or rename fields in a document.
     */
    @ThreadSafe
    public static interface FieldFilter {

        /**
         * Applies this filter to the given document to exclude or rename fields.
         *
         * @param doc document to exclude or rename fields
         * @return modified document
         */
        Document apply(Document doc);

        /**
        * Applies this filter to the given document to exclude or rename fields.
        *
        * @param doc document to exclude or rename fields
        * @return modified document
        */
        BsonDocument apply(BsonDocument doc);

        /**
        * Applies this filter to the given change document to exclude or rename fields.
        *
        * @param doc document to exclude or rename fields
        * @return modified document
        */
        BsonDocument applyChange(BsonDocument doc);

        /**
        * Applies this filter to the full name of field to exclude or rename field.
        *
        * @param field the original name of field
        * @return the new field name or {@code null} if the field should be removed
        */
        String apply(String field);
    }

    /**
     * The configured exclusion/renaming patterns.
     */
    private final List<Path> paths;

    private FieldSelector(List<Path> paths) {
        this.paths = paths;
    }

    /**
     * A builder of a field selector.
     */
    public static class FieldSelectorBuilder {

        private String fullyQualifiedFieldNames;
        private String fullyQualifiedFieldReplacements;

        private FieldSelectorBuilder() {
        }

        /**
         * Specifies the comma-separated list of fully-qualified field names that should be included.
         *
         * @param fullyQualifiedFieldNames the comma-separated list of fully-qualified field names to exclude; may be
         *            {@code null} or empty
         * @return this builder so that methods can be chained together; never {@code null}
         */
        public FieldSelectorBuilder excludeFields(String fullyQualifiedFieldNames) {
            this.fullyQualifiedFieldNames = fullyQualifiedFieldNames;
            return this;
        }

        /**
         * Specifies the comma-separated list of fully-qualified field replacements to rename fields.
         *
         * @param fullyQualifiedFieldReplacements the comma-separated list of fully-qualified field replacements to rename
         *            fields; may be {@code null} or empty
         * @return this builder so that methods can be chained together; never {@code null}
         */
        public FieldSelectorBuilder renameFields(String fullyQualifiedFieldReplacements) {
            this.fullyQualifiedFieldReplacements = fullyQualifiedFieldReplacements;
            return this;
        }

        /**
         * Builds the filter selector that returns the field filter for a given collection identifier, using the comma-separated
         * list of fully-qualified field names (for details, see {@link MongoDbConnectorConfig#FIELD_EXCLUDE_LIST}) defining
         * which fields (if any) should be excluded, and using the comma-separated list of fully-qualified field replacements
         * (for details, see {@link MongoDbConnectorConfig#FIELD_RENAMES}) defining which fields (if any) should be
         * renamed.
         *
         * @return the filter selector that returns the filter to exclude or rename fields in a document
         */
        public FieldSelector build() {
            List<Path> result = new ArrayList<>();
            parse(fullyQualifiedFieldNames, name -> {
                String[] nameNodes = parseIntoParts(name, name, length -> length < 3, DOT);
                return new RemovePath(selectNamespacePartAsPattern(nameNodes), selectFieldPartAsNodes(nameNodes));
            }, result);
            parse(fullyQualifiedFieldReplacements, name -> {
                String[] replacement = parseIntoParts(name, name, length -> length != 2, COLON);
                String[] nameNodes = parseIntoParts(name, replacement[0], length -> length < 3, DOT);
                return new RenamePath(selectNamespacePartAsPattern(nameNodes), selectFieldPartAsNodes(nameNodes), replacement[1]);
            }, result);
            return new FieldSelector(result);
        }

        private void parse(String value, Function<String, Path> pathCreator, List<Path> result) {
            if (!Strings.isNullOrEmpty(value)) {
                Arrays.stream(value.trim().split(",")).forEach(name -> result.add(pathCreator.apply(name)));
            }
        }

        static String[] parseIntoParts(String name, String value, Predicate<Integer> lengthPredicate, Pattern delimiterPattern) {
            String[] nodes = delimiterPattern.split(value.trim());
            if (lengthPredicate.test(nodes.length) || Arrays.stream(nodes).anyMatch(Strings::isNullOrEmpty)) {
                throw new ConfigException("Invalid format: " + name);
            }
            return nodes;
        }

        private Pattern selectNamespacePartAsPattern(String[] expressionNodes) {
            String databaseName = expressionNodes[0];
            String collectionName = expressionNodes[1];
            String namespaceRegex = toRegex(databaseName) + "\\." + toRegex(collectionName);
            return Pattern.compile(namespaceRegex.trim(), Pattern.CASE_INSENSITIVE);
        }

        private String toRegex(String value) {
            return value.replace("*", ".*");
        }

        private String[] selectFieldPartAsNodes(String[] expressionNodes) {
            return Arrays.copyOfRange(expressionNodes, 2, expressionNodes.length);
        }
    }

    /**
     * Returns a new {@link FieldSelectorBuilder builder} for a field selector.
     *
     * @return the builder; never {@code null}
     */
    public static FieldSelectorBuilder builder() {
        return new FieldSelectorBuilder();
    }

    /**
     * Returns the field filter for the given collection identifier.
     *
     * <p>
     * Note that the field filter is completely independent of the collection selection predicate, so it is expected that this
     * filter be used only after the collection selection predicate determined the collection containing documents
     * with the field(s) is to be used.
     *
     * @param id the collection identifier, never {@code null}
     * @return the field filter, never {@code null}
     */
    public FieldFilter fieldFilterFor(CollectionId id) {
        if (!paths.isEmpty()) {
            String namespace = id.namespace();
            List<Path> pathsApplyingToCollection = paths.stream()
                    .filter(path -> path.matches(namespace))
                    .collect(Collectors.toList());
            final Map<String, Path> pathsByAddress = paths.stream()
                    .collect(Collectors.toMap(Path::toString, Function.identity()));
            if (pathsApplyingToCollection.size() == 1) {
                return new FieldFilter() {
                    final Path path = pathsApplyingToCollection.get(0);

                    @Override
                    public String apply(String field) {
                        return path.matchesPath(field) ? path.generateNewFieldName(field) : field;
                    }

                    @Override
                    public BsonDocument apply(BsonDocument doc) {
                        path.modify((Map) doc, null, null);
                        return doc;
                    }

                    @Override
                    public Document apply(Document doc) {
                        Document setDoc = doc.get("$set", Document.class);
                        Document unsetDoc = doc.get("$unset", Document.class);
                        pathsApplyingToCollection.get(0).modify(doc, setDoc, unsetDoc);
                        return doc;
                    }

                    @Override
                    public BsonDocument applyChange(BsonDocument doc) {
                        path.modify(null, (Map) doc, null);
                        return doc;
                    }
                };
            }
            else if (pathsApplyingToCollection.size() > 1) {
                return new FieldFilter() {

                    @Override
                    public String apply(String field) {
                        for (Path p : pathsApplyingToCollection) {
                            if (p.matchesPath(field)) {
                                return p.generateNewFieldName(field);
                            }
                        }
                        return field;
                    }

                    @Override
                    public BsonDocument apply(BsonDocument doc) {
                        pathsApplyingToCollection.forEach(path -> path.modify((Map) doc, null, null));
                        return doc;
                    }

                    @Override
                    public Document apply(Document doc) {
                        Document setDoc = doc.get("$set", Document.class);
                        Document unsetDoc = doc.get("$unset", Document.class);
                        pathsApplyingToCollection.forEach(path -> path.modify(doc, setDoc, unsetDoc));
                        return doc;
                    }

                    @Override
                    public BsonDocument applyChange(BsonDocument doc) {
                        pathsApplyingToCollection.forEach(path -> path.modify(null, (Map) doc, null));
                        return doc;
                    }
                };
            }
        }
        return new FieldFilter() {

            @Override
            public String apply(String field) {
                return field;
            }

            @Override
            public BsonDocument apply(BsonDocument doc) {
                return doc;
            }

            @Override
            public Document apply(Document doc) {
                return doc;
            }

            @Override
            public BsonDocument applyChange(BsonDocument doc) {
                return doc;
            }
        };
    }

    private static final class FieldNameAndValue {

        private final String key;
        private final Object value;

        private FieldNameAndValue(String key, Object value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * Represents a field that should be excluded from or renamed in MongoDB documents.
     */
    @ThreadSafe
    private static abstract class Path {

        final Pattern namespacePattern;
        final String[] fieldNodes;
        final String field;

        private Path(Pattern namespacePattern, String[] fieldNodes) {
            this.namespacePattern = namespacePattern;
            this.fieldNodes = fieldNodes;
            StringJoiner field = new StringJoiner(".");
            Arrays.stream(fieldNodes).forEach(field::add);
            this.field = field.toString();
        }

        /**
         * Whether this path applies to the given collection namespace or not.
         *
         * @param namespace namespace to match
         * @return {@code true} if this path applies to the given collection namespace
         */
        public boolean matches(String namespace) {
            return namespacePattern.matcher(namespace).matches();
        }

        /**
         * Applies the transformation represented by this path, i.e. removes or renames the represented field.
         *
         * @param doc      the original document; never {@code null}
         * @param setDoc   the value of {@code $set} field; may be {@code null}
         * @param unsetDoc the value of {@code $unset} field; may be {@code null}
         */
        public void modify(Map<String, Object> doc, Map<String, Object> setDoc, Map<String, Object> unsetDoc) {
            if (setDoc == null && unsetDoc == null) {
                modifyFields(doc, fieldNodes, 0);
            }
            else {
                if (setDoc != null) {
                    modifyFieldsWithDotNotation(setDoc);
                }
                if (unsetDoc != null) {
                    modifyFieldsWithDotNotation(unsetDoc);
                }
            }
        }

        /**
         * Modifies fields in the document by the given path nodes start with the begin index.
         *
         * <p>
         * Note that the path doesn't support modification of fields inside arrays of arrays.
         *
         * @param doc        the document to modify fields
         * @param nodes      the path nodes
         * @param beginIndex the begin index
         */
        private void modifyFields(Map<String, Object> doc, String[] nodes, int beginIndex) {
            Map<String, Object> current = doc;
            int stopIndex = nodes.length - 1;
            for (int i = beginIndex; i < nodes.length; i++) {
                String node = nodes[i];
                if (i == stopIndex) {
                    modifyField(current, node);
                    break;
                }
                else {
                    Object next = current.get(node);
                    if (next instanceof Map<?, ?>) {
                        current = (Map<String, Object>) next;
                    }
                    else {
                        modifyFields(next, nodes, i + 1);
                        break;
                    }
                }
            }
        }

        /**
         * Modifies fields that use the dot notation, like {@code 'a.b'} or {@code 'a.0.b'}.
         *
         * <p>
         * First, we try to modify field that exactly matches the current path. Then, if the field isn't found, we try to find
         * fields that start with the current path or fields that are part of the current path.
         *
         * @param doc the document to modify fields
         */
        private void modifyFieldsWithDotNotation(Map<String, Object> doc) {
            // document can contain null value by key
            if (doc.containsKey(field)) {
                modifyFieldWithDotNotation(doc, field);
                return;
            }

            List<FieldNameAndValue> newFields = null;
            Iterator<Map.Entry<String, Object>> it = doc.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Object> entry = it.next();
                String[] originKeyNodes = DOT.split(entry.getKey());
                String[] keyNodes = excludeNumericItems(originKeyNodes);
                if (keyNodes.length > fieldNodes.length) {
                    // field is a prefix of key, e.g. field is 'a' and key is 'a.b'
                    if (startsWith(fieldNodes, keyNodes)) {
                        newFields = add(newFields, generateNewFieldName(originKeyNodes, entry.getValue()));
                        it.remove();
                    }
                }
                else if (keyNodes.length < fieldNodes.length) {
                    // key is a prefix of field, e.g. field is 'a.b' and key is 'a'
                    if (startsWith(keyNodes, fieldNodes)) {
                        Object value = entry.getValue();
                        if (value instanceof Map<?, ?>) {
                            modifyFields((Map<String, Object>) value, fieldNodes, keyNodes.length);
                        }
                        else {
                            modifyFields(value, fieldNodes, keyNodes.length);
                        }
                        break;
                    }
                }
                else {
                    // key is equal to field, e.g. field is 'a' and key is 'a'
                    if (Arrays.equals(keyNodes, fieldNodes)) {
                        newFields = add(newFields, generateNewFieldName(originKeyNodes, entry.getValue()));
                        it.remove();
                    }
                }
            }

            if (newFields != null) {
                newFields.forEach(entry -> doc.put(checkFieldExists(doc, entry.key), entry.value));
            }
        }

        private void modifyFields(Object value, String[] fieldNodes, int length) {
            if (value instanceof List) {
                for (Object item : (List<?>) value) {
                    if (item instanceof Map<?, ?>) {
                        modifyFields((Map<String, Object>) item, fieldNodes, length);
                    }
                }
            }
        }

        /**
         * Excludes numeric items from the given array.
         *
         * <p>
         * If the array has consecutive numeric items, like {@code 'a.0.0.b'}, then the numeric items aren't excluded.
         * It is necessary because the modification of fields inside arrays of arrays isn't supported.
         *
         * @param items the array to exclude numeric items
         * @return filtered items
         */
        private String[] excludeNumericItems(String[] items) {
            if (items.length > 1) {
                List<String> newItems = null;
                boolean previousNumerical = false;
                int offset = 0;
                for (int i = 0; i < items.length; i++) {
                    if (Strings.isNumeric(items[i])) {
                        if (previousNumerical) {
                            return items;
                        }
                        previousNumerical = true;
                        if (newItems == null) {
                            newItems = new ArrayList<>(Arrays.asList(items));
                        }
                        newItems.remove(i - offset++);
                    }
                    else {
                        previousNumerical = false;
                    }
                }
                if (newItems != null) {
                    return newItems.toArray(new String[newItems.size()]);
                }
            }
            return items;
        }

        /**
         * Returns {@code true} if the source array starts with the specified array.
         *
         * @param begin  the array from which the source array begins
         * @param source the source array to check
         * @return {@code true} if the source array starts with the specified array
         */
        private boolean startsWith(String[] begin, String[] source) {
            if (begin.length > source.length) {
                return false;
            }
            for (int i = 0; i < begin.length; i++) {
                if (!source[i].equals(begin[i])) {
                    return false;
                }
            }
            return true;
        }

        private <T> List<T> add(List<T> list, T element) {
            if (element != null) {
                if (list == null) {
                    list = new ArrayList<>();
                }
                list.add(element);
            }
            return list;
        }

        String checkFieldExists(Map<String, Object> doc, String field) {
            if (doc.containsKey(field)) {
                throw new IllegalArgumentException("Document already contains field : " + field);
            }
            return field;
        }

        /**
         * Modifies the field in the document used for read, insert and full update operations.
         *
         * @param doc   the document to modify field
         * @param field the modified field
         */
        abstract void modifyField(Map<String, Object> doc, String field);

        /**
         * Immediately modifies the field that uses the dot notation like {@code 'a.b'} in the document used for set and
         * unset update operations.
         *
         * @param doc   the document to modify field
         * @param field the modified field
         */
        abstract void modifyFieldWithDotNotation(Map<String, Object> doc, String field);

        /**
         * Generates a new field name for the given value.
         *
         * @param fieldNodes the field nodes
         * @param value      the field value
         * @return a new field name for the given value
         */
        abstract FieldNameAndValue generateNewFieldName(String[] fieldNodes, Object value);

        /**
         * Generates a new field name.
         *
         * @param fieldName the original field name
         * @return a new field name
         */
        abstract String generateNewFieldName(String fieldName);

        /**
         * Verifies whether a parameter representing path is the same or belongs under this path.
         * 
         * @param other - the string representing the other path
         * @return - true if this path is the same or parent of the path passed
         */
        public boolean matchesPath(String other) {
            final String[] otherParts = excludeNumericItems(FieldSelectorBuilder.parseIntoParts(other, other, length -> length < 1, DOT));
            if (fieldNodes.length <= other.length()) {
                for (int i = 0; i < fieldNodes.length; i++) {
                    if (!fieldNodes[i].equals(otherParts[i])) {
                        return false;
                    }
                }
            }
            return true;
        }

        @Override
        public String toString() {
            return field;
        }
    }

    @ThreadSafe
    private static final class RemovePath extends Path {

        private RemovePath(Pattern namespacePattern, String[] fieldNodes) {
            super(namespacePattern, fieldNodes);
        }

        @Override
        void modifyField(Map<String, Object> doc, String field) {
            doc.remove(field);
        }

        @Override
        void modifyFieldWithDotNotation(Map<String, Object> doc, String field) {
            doc.remove(field);
        }

        @Override
        FieldNameAndValue generateNewFieldName(String[] fieldNodes, Object value) {
            // this path doesn't support new field name generation
            return null;
        }

        @Override
        String generateNewFieldName(String fieldName) {
            // this path doesn't support new field name generation
            return null;
        }
    }

    @ThreadSafe
    private static final class RenamePath extends Path {

        // field node that is used to change the old field in document. It can't be nested field
        private final String newFieldNode;
        // field that is used to replace the old field in document. It can be top level or nested field
        private final String newField;

        private RenamePath(Pattern namespacePattern, String[] oldFieldNodes, String newFieldNode) {
            super(namespacePattern, oldFieldNodes);
            this.newFieldNode = newFieldNode;
            this.newField = replaceLastNameNode(oldFieldNodes, newFieldNode);
        }

        @Override
        void modifyField(Map<String, Object> doc, String field) {
            // if the original field does not exist, make no change
            if (!doc.containsKey(field)) {
                return;
            }
            doc.put(checkFieldExists(doc, newFieldNode), doc.remove(field));
        }

        @Override
        void modifyFieldWithDotNotation(Map<String, Object> doc, String field) {
            // if the original field does not exist, make no change
            if (!doc.containsKey(field)) {
                return;
            }
            doc.put(checkFieldExists(doc, newField), doc.remove(field));
        }

        @Override
        FieldNameAndValue generateNewFieldName(String[] fieldNodes, Object value) {
            String newField = rename(fieldNodes);
            return new FieldNameAndValue(newField, value);
        }

        @Override
        String generateNewFieldName(String fieldName) {
            return rename(FieldSelectorBuilder.parseIntoParts(fieldName, fieldName, length -> length < 1, DOT));
        }

        /**
         * Replaces a last name node in the given name nodes, if the name nodes contain only one node,
         * the last name node is returned.
         *
         * @param nameNodes    the name nodes to replace
         * @param lastNameNode the last name node
         * @return replaced name or last name node
         */
        private String replaceLastNameNode(String[] nameNodes, String lastNameNode) {
            if (nameNodes.length > 1) {
                StringJoiner newName = new StringJoiner(".");
                int replaceIndex = nameNodes.length - 1;
                for (int i = 0; i < nameNodes.length; i++) {
                    newName.add((i == replaceIndex) ? lastNameNode : nameNodes[i]);
                }
                return newName.toString();
            }
            return lastNameNode;
        }

        private String rename(String[] nameNodes) {
            StringJoiner newName = new StringJoiner(".");
            int replaceIndex = fieldNodes.length - 1;
            int i = 0;
            for (String nameNode : nameNodes) {
                if (Strings.isNumeric(nameNode)) {
                    newName.add(nameNode);
                }
                else {
                    newName.add((i == replaceIndex) ? newFieldNode : nameNode);
                    i++;
                }
            }
            return newName.toString();
        }
    }
}
