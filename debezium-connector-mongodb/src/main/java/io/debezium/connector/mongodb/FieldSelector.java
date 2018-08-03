/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import io.debezium.annotation.ThreadSafe;
import io.debezium.util.Strings;
import org.apache.kafka.common.config.ConfigException;
import org.bson.Document;

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

/**
 * This filter selector is designed to determine the filter to exclude or rename fields in a document.
 */
@ThreadSafe
public final class FieldSelector {

    /**
     * This filter is designed to exclude or rename fields in a document.
     */
    @ThreadSafe
    @FunctionalInterface
    public static interface FieldFilter {

        /**
         * Applies this filter to the given document to exclude or rename fields.
         *
         * @param doc document to exclude or rename fields
         * @return modified document
         */
        Document apply(Document doc);
    }

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
         * list of fully-qualified field names (for details, see {@link MongoDbConnectorConfig#FIELD_BLACKLIST}) defining
         * which fields (if any) should be excluded, and using the comma-separated list of fully-qualified field replacements
         * (for details, see {@link MongoDbConnectorConfig#FIELD_RENAMES}) defining which fields (if any) should be
         * renamed.
         *
         * @return the filter selector that returns the filter to exclude or rename fields in a document
         */
        public FieldSelector build() {
            List<Path> result = new ArrayList<>();
            parse(fullyQualifiedFieldNames, name -> {
                String[] fieldNodes = parseIntoParts(name, name, length -> length < 3, "\\.");
                return new RemovePath(fieldNodes);
            }, result);
            parse(fullyQualifiedFieldReplacements, name -> {
                String[] renameMapping = parseIntoParts(name, name, length -> length != 2, "=");
                String[] fieldNodes = parseIntoParts(name, renameMapping[0], length -> length < 3, "\\.");
                return new RenamePath(fieldNodes, renameMapping[1]);
            }, result);
            return new FieldSelector(result);
        }

        private void parse(String value, Function<String, Path> pathCreator, List<Path> result) {
            if (!Strings.isNullOrEmpty(value)) {
                Arrays.stream(value.split(",")).forEach(name -> result.add(pathCreator.apply(name)));
            }
        }

        private String[] parseIntoParts(String name, String value, Predicate<Integer> lengthPredicate, String delimiter) {
            String[] fieldNodes = value.split(delimiter);
            if (lengthPredicate.test(fieldNodes.length) || Arrays.stream(fieldNodes).anyMatch(Strings::isNullOrEmpty)) {
                throw new ConfigException("Invalid format: " + name);
            }
            return fieldNodes;
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
     * Returns the field filter for a given collection identifier.
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
            List<Path> result = paths.stream().filter(path -> path.supports(namespace)).collect(Collectors.toList());
            if (result.size() == 1) {
                return doc -> {
                    Document setDoc = doc.get("$set", Document.class);
                    Document unsetDoc = doc.get("$unset", Document.class);
                    result.get(0).modify(doc, setDoc, unsetDoc);
                    return doc;
                };
            } else if (result.size() > 1) {
                return doc -> {
                    Document setDoc = doc.get("$set", Document.class);
                    Document unsetDoc = doc.get("$unset", Document.class);
                    result.forEach(path -> path.modify(doc, setDoc, unsetDoc));
                    return doc;
                };
            }
        }
        return doc -> doc;
    }

    private static final class Entry {

        private final String key;
        private final Object value;

        private Entry(String key, Object value) {
            this.key = key;
            this.value = value;
        }
    }

    @ThreadSafe
    private static interface Path {

        boolean supports(String namespace);

        void modify(Document doc, Document setDoc, Document unsetDoc);
    }

    @ThreadSafe
    private static abstract class AbstractPath implements Path {

        final Pattern namespacePattern;
        final String[] fieldNodes;
        final String field;

        private AbstractPath(String[] fieldNodes) {
            this.namespacePattern = namespacePattern(fieldNodes[0], fieldNodes[1]);
            this.fieldNodes = Arrays.copyOfRange(fieldNodes, 2, fieldNodes.length);
            StringJoiner field = new StringJoiner(".");
            Arrays.stream(fieldNodes).forEach(field::add);
            this.field = field.toString();
        }

        private Pattern namespacePattern(String databaseName, String collectionName) {
            String namespaceRegex = toRegex(databaseName) + "\\." + toRegex(collectionName);
            return Pattern.compile(namespaceRegex, Pattern.CASE_INSENSITIVE);
        }

        private String toRegex(String value) {
            return value.replace("*", ".*");
        }

        @Override
        public boolean supports(String namespace) {
            return namespacePattern.matcher(namespace).matches();
        }

        @Override
        public void modify(Document doc, Document setDoc, Document unsetDoc) {
            if (setDoc == null && unsetDoc == null) {
                modifyFields(doc, fieldNodes, 0);
            } else {
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
        private void modifyFields(Document doc, String[] nodes, int beginIndex) {
            Document current = doc;
            int stopIndex = nodes.length - 1;
            for (int i = beginIndex; i < nodes.length; i++) {
                String node = nodes[i];
                if (i == stopIndex) {
                    modifyField(current, node);
                    break;
                } else {
                    Object next = current.get(node);
                    if (next instanceof Document) {
                        current = (Document) next;
                    } else {
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
        private void modifyFieldsWithDotNotation(Document doc) {
            // document can contain null value by key
            if (doc.containsKey(field)) {
                modifyFieldWithDotNotation(doc, field);
                return;
            }
            List<Entry> deferred = null;
            Iterator<Map.Entry<String, Object>> it = doc.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Object> entry = it.next();
                String[] originKeyNodes = entry.getKey().split("\\.");
                String[] keyNodes = excludeNumericItems(originKeyNodes);
                if (keyNodes.length > fieldNodes.length) {
                    // field is a prefix of key, e.g. field is 'a' and key is 'a.b'
                    if (startsWith(fieldNodes, keyNodes)) {
                        deferred = modifyFieldDeferred(deferred, originKeyNodes, entry.getValue());
                        it.remove();
                    }
                } else if (keyNodes.length < fieldNodes.length) {
                    // key is a prefix of field, e.g. field is 'a.b' and key is 'a'
                    if (startsWith(keyNodes, fieldNodes)) {
                        Object value = entry.getValue();
                        if (value instanceof Document) {
                            modifyFields((Document) value, fieldNodes, keyNodes.length);
                        } else {
                            modifyFields(value, fieldNodes, keyNodes.length);
                        }
                        break;
                    }
                } else {
                    // key is equal to field, e.g. field is 'a' and key is 'a'
                    if (Arrays.equals(keyNodes, fieldNodes)) {
                        deferred = modifyFieldDeferred(deferred, originKeyNodes, entry.getValue());
                        it.remove();
                    }
                }
            }
            if (deferred != null) {
                deferred.forEach(entry -> doc.put(checkFieldExists(doc, entry.key), entry.value));
            }
        }

        private void modifyFields(Object value, String[] fieldNodes, int length) {
            if (value instanceof List) {
                for (Object item : (List) value) {
                    if (item instanceof Document) {
                        modifyFields((Document) item, fieldNodes, length);
                    }
                }
            }
        }

        /**
         * Excludes numeric items from a given array.
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
                    } else {
                        previousNumerical = false;
                    }
                }
                if (newItems != null) {
                    String[] result = new String[newItems.size()];
                    return newItems.toArray(result);
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

        String checkFieldExists(Document doc, String field) {
            if (doc.containsKey(field)) {
                throw new IllegalArgumentException("Document already contains field : " + field);
            }
            return field;
        }

        abstract List<Entry> modifyFieldDeferred(List<Entry> deferred, String[] fieldNodes, Object value);

        abstract void modifyField(Document doc, String field);

        abstract void modifyFieldWithDotNotation(Document doc, String field);

        @Override
        public String toString() {
            return field;
        }
    }

    @ThreadSafe
    private static final class RemovePath extends AbstractPath {

        private RemovePath(String[] fieldNodes) {
            super(fieldNodes);
        }

        @Override
        List<Entry> modifyFieldDeferred(List<Entry> deferred, String[] fieldNodes, Object value) {
            // this path doesn't support deferred field modification
            return deferred;
        }

        @Override
        void modifyField(Document doc, String field) {
            doc.remove(field);
        }

        @Override
        void modifyFieldWithDotNotation(Document doc, String field) {
            doc.remove(field);
        }
    }

    @ThreadSafe
    private static final class RenamePath extends AbstractPath {

        // field node that is used to change the old field in document. It can't be nested field
        private final String newFieldNode;
        // field that is used to replace the old field in document. It can be top level or nested field
        private final String newField;

        private RenamePath(String[] oldFieldNodes, String newFieldNode) {
            super(oldFieldNodes);
            this.newFieldNode = newFieldNode;
            this.newField = replaceLastNameNode(oldFieldNodes, newFieldNode);
        }

        @Override
        void modifyField(Document doc, String field) {
            doc.put(checkFieldExists(doc, newFieldNode), doc.remove(field));
        }

        @Override
        void modifyFieldWithDotNotation(Document doc, String field) {
            doc.put(checkFieldExists(doc, newField), doc.remove(field));
        }

        @Override
        List<Entry> modifyFieldDeferred(List<Entry> deferred, String[] fieldNodes, Object value) {
            String newField = rename(fieldNodes);
            if (deferred == null) {
                deferred = new ArrayList<>();
            }
            deferred.add(new Entry(newField, value));
            return deferred;
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
                } else {
                    newName.add((i == replaceIndex) ? newFieldNode : nameNode);
                    i++;
                }
            }
            return newName.toString();
        }
    }
}
