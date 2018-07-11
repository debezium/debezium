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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * This filter selector is designed to determine the filter to exclude fields from document.
 */
@ThreadSafe
public final class FieldSelector {
    /**
     * This filter is designed to exclude fields from document.
     */
    @ThreadSafe
    @FunctionalInterface
    public static interface FieldFilter {
        /**
         * Apply this filter to the given document to exclude fields.
         *
         * @param doc document to exclude fields
         * @return modified document
         */
        Document apply(Document doc);
    }

    private static final FieldSelector EMPTY = new FieldSelector();
    private final List<Filter> filters;

    private FieldSelector() {
        filters = Collections.emptyList();
    }

    private FieldSelector(List<Filter> filters) {
        this.filters = filters;
    }

    /**
     * Builds the filter selector that returns the field filter for a given collection identifier, using the given comma-separated
     * list of fully-qualified field names (for details, see {@link MongoDbConnectorConfig#FIELD_BLACKLIST}) defining
     * which fields (if any) should be <i>excluded</i>.
     *
     * @param fullyQualifiedFieldNames the comma-separated list of fully-qualified field names to exclude; may be null or
     *            empty
     * @return the filter selector that returns the filter to exclude fields from document
     */
    public static FieldSelector excludeFields(String fullyQualifiedFieldNames) {
        if (Strings.isNullOrEmpty(fullyQualifiedFieldNames)) {
            return EMPTY;
        }
        String[] names = fullyQualifiedFieldNames.split(",");
        List<Filter> filters = new ArrayList<>(names.length);
        for (String name : names) {
            String[] parts = name.split(":");
            if (parts.length != 2 || Strings.isNullOrEmpty(parts[0]) || Strings.isNullOrEmpty(parts[1])) {
                throw new ConfigException("Invalid format: " + name);
            }
            Pattern pattern = Pattern.compile(parts[0], Pattern.CASE_INSENSITIVE);
            String[] fields = parts[1].split("\\|");
            List<Path> paths = new ArrayList<>(fields.length);
            for (String field : fields) {
                paths.add(new Path(field));
            }
            filters.add(new Filter(pattern, paths));
        }
        return new FieldSelector(filters);
    }

    /**
     * Returns the field filter for a given collection identifier.
     *
     * <p>
     * Note that the field filter is completely independent of the collection selection predicate, so it is expected that this filter
     * be used only <i>after</i> the collection selection predicate determined the collection containing documents with
     * the field(s) is to be used.
     *
     * @param id the collection identifier, never {@code null}
     * @return the field filter, never {@code null}
     */
    public FieldFilter fieldFilterFor(CollectionId id) {
        if (!filters.isEmpty()) {
            List<Path> paths = new ArrayList<>();
            String namespace = id.namespace();
            for (Filter filter : filters) {
                if (filter.pattern.matcher(namespace).matches()) {
                    paths.addAll(filter.paths);
                }
            }
            if (paths.size() == 1) {
                return paths.get(0)::remove;
            } else if (paths.size() > 1) {
                return doc -> {
                    Document setDoc = doc.get("$set", Document.class);
                    Document unsetDoc = doc.get("$unset", Document.class);
                    paths.forEach(path -> path.remove(doc, setDoc, unsetDoc));
                    return doc;
                };
            }
        }
        return doc -> doc;
    }

    @ThreadSafe
    private static final class Filter {
        private final Pattern pattern;
        private final List<Path> paths;

        private Filter(Pattern pattern, List<Path> paths) {
            this.pattern = pattern;
            this.paths = paths;
        }
    }

    @ThreadSafe
    private static final class Path {

        private final String field;
        private final String[] fieldNodes;

        private Path(String field) {
            this.field = field;
            this.fieldNodes = field.split("\\.");
        }

        private Document remove(Document doc) {
            Document setDoc = doc.get("$set", Document.class);
            Document unsetDoc = doc.get("$unset", Document.class);
            remove(doc, setDoc, unsetDoc);
            return doc;
        }

        private void remove(Document doc, Document setDoc, Document unsetDoc) {
            if (setDoc == null && unsetDoc == null) {
                removeFields(doc, fieldNodes, 0);
            } else {
                if (setDoc != null) {
                    removeFieldsWithDotNotation(setDoc);
                }
                if (unsetDoc != null) {
                    removeFieldsWithDotNotation(unsetDoc);
                }
            }
        }

        /**
         * Removes fields from the document by the given path nodes start with the begin index.
         *
         * <p>
         *  Note that the path does not support removing fields inside arrays of arrays.
         *
         * @param doc document to remove fields
         * @param nodes path nodes
         * @param beginIndex begin index
         */
        private void removeFields(Document doc, String[] nodes, int beginIndex) {
            Document current = doc;
            int stop = nodes.length - 1;
            for (int i = beginIndex; i < nodes.length; i++) {
                String node = nodes[i];
                if (i == stop) {
                    current.remove(node);
                } else {
                    Object next = current.get(node);
                    if (next instanceof Document) {
                        current = (Document) next;
                    } else {
                        if (next instanceof List) {
                            for (Object item : (List) next) {
                                if (item instanceof Document) {
                                    removeFields((Document) item, nodes, i + 1);
                                }
                            }
                        }
                        break;
                    }
                }
            }
        }

        /**
         * Removes fields that uses the dot notation, like {@code 'a.b'} or {@code 'a.0.b'}.
         *
         * <p>
         * First, we try to delete field that exactly matches the current path. Then, if the field is not found, we try to find
         * fields that start with the current path or fields that are part of the current path.
         *
         * @param doc document to remove fields
         */
        private void removeFieldsWithDotNotation(Document doc) {
            // document can contain null value by key
            if (doc.containsKey(field)) {
                doc.remove(field);
                return;
            }
            Iterator<Map.Entry<String, Object>> it = doc.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Object> entry = it.next();
                String[] keyNodes = buildKeyNodes(entry.getKey());
                if (keyNodes.length > fieldNodes.length) {
                    // field is a prefix of key, e.g. field is 'a' and key is 'a.b'
                    if (startsWith(fieldNodes, keyNodes)) {
                        it.remove();
                    }
                } else if (keyNodes.length < fieldNodes.length) {
                    // key is a prefix of field, e.g. field is 'a.b' and key is 'a'
                    if (startsWith(keyNodes, fieldNodes)) {
                        Object value = entry.getValue();
                        if (value instanceof Document) {
                            removeFields((Document) value, fieldNodes, keyNodes.length);
                        } else if (value instanceof List) {
                            for (Object item : (List) value) {
                                if (item instanceof Document) {
                                    removeFields((Document) item, fieldNodes, keyNodes.length);
                                }
                            }
                        }
                        break;
                    }
                } else {
                    // key is equal to field, e.g. field is 'a' and key is 'a'
                    if (Arrays.equals(keyNodes, fieldNodes)) {
                        it.remove();
                    }
                }
            }
        }

        /**
         * Builds key nodes from which numeric nodes are excluded.
         *
         * <p>
         * If the key has consecutive numeric nodes, like {@code 'a.0.0.b'}, then the numeric nodes are not excluded.
         * It is necessary because the path does not support removing fields inside arrays of arrays.
         *
         * @param key the key to build nodes
         * @return key nodes
         */
        private String[] buildKeyNodes(String key) {
            String[] nodes = key.split("\\.");
            if (nodes.length > 1) {
                List<String> newNodes = null;
                boolean previousNumerical = false;
                int offset = 0;
                for (int i = 0; i < nodes.length; i++) {
                    if (Strings.isNumeric(nodes[i])) {
                        if (previousNumerical) {
                            return nodes;
                        }
                        previousNumerical = true;
                        if (newNodes == null) {
                            newNodes = new ArrayList<>(Arrays.asList(nodes));
                        }
                        newNodes.remove(i - offset++);
                    } else {
                        previousNumerical = false;
                    }
                }
                if (newNodes != null) {
                    String[] result = new String[newNodes.size()];
                    return newNodes.toArray(result);
                }
            }
            return nodes;
        }

        /**
         * Returns {@code true} if the source array starts with the specified array.
         *
         * @param begin array from which the source array begins
         * @param source source array to check
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

        @Override
        public String toString() {
            return field;
        }
    }
}
