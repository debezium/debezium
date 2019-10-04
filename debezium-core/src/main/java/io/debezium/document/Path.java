/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import java.util.Optional;
import java.util.function.Consumer;

import io.debezium.annotation.Immutable;

/**
 * A representation of multiple name segments that together form a path within {@link Document}.
 *
 * @author Randall Hauch
 */
@Immutable
public interface Path extends Iterable<String> {

    public static interface Segments {
        public static boolean isAfterLastIndex(String segment) {
            return "-".equals(segment);
        }

        public static boolean isArrayIndex(String segment) {
            return isAfterLastIndex(segment) || asInteger(segment).isPresent();
        }

        public static boolean isFieldName(String segment) {
            return !isArrayIndex(segment);
        }

        public static Optional<Integer> asInteger(String segment) {
            try {
                return Optional.of(Integer.valueOf(segment));
            }
            catch (NumberFormatException e) {
                return Optional.empty();
            }
        }

        public static Optional<Integer> asInteger(Optional<String> segment) {
            return segment.isPresent() ? asInteger(segment.get()) : Optional.empty();
        }
    }

    /**
     * Get the zero-length path.
     *
     * @return the shared root path; never null
     */
    public static Path root() {
        return Paths.RootPath.INSTANCE;
    }

    /**
     * Get an {@link Optional} reference to the root path. The resulting Optional will always {@link Optional#isPresent() be
     * present}.
     *
     * @return the shared optional root path; never null
     */
    static Optional<Path> optionalRoot() {
        return Paths.RootPath.OPTIONAL_OF_ROOT;
    }

    /**
     * Parse a JSON Path expression. Segments are separated by a single forward slash ('{@code /}'); any '{@code ~}' or '{@code /}
     * ' literals must be escaped. Trailing slashes are ignored.
     *
     * @param path the path as a string; may not be null, but may be an empty string or "/" for a root path
     * @return the path object; never null
     */
    static Path parse(String path) {
        return Paths.parse(path, true);
    }

    /**
     * Parse a JSON Path expression. Segments are separated by a single forward slash ('{@code /}'); any '{@code ~}' or '{@code /}
     * ' literals must be escaped. Trailing slashes are ignored.
     *
     * @param path the path as a string; may not be null, but may be an empty string or "/" for a root path
     * @param resolveJsonPointerEscapes {@code true} if '{@code ~}' and '{@code /} ' literals are to be escaped as '{@code ~0}'
     *            and '{@code ~1}', respectively, or {@code false} if they are not to be escaped
     * @return the path object; never null
     */
    static Path parse(String path, boolean resolveJsonPointerEscapes) {
        return Paths.parse(path, resolveJsonPointerEscapes);
    }

    /**
     * Return whether this path is the root path with no segments. This method is equivalent to {@code size() == 0}.
     *
     * @return true if this path contains exactly one segment, or false otherwise
     */
    default boolean isRoot() {
        return size() == 0;
    }

    /**
     * Return whether this path has a single segment. This method is equivalent to {@code size() == 1}.
     *
     * @return true if this path contains exactly one segment, or false otherwise
     */
    default boolean isSingle() {
        return size() == 1;
    }

    /**
     * Return whether this path has more than one segment. This method is equivalent to {@code size() > 1}.
     *
     * @return true if this path contains exactly one segment, or false otherwise
     */
    default boolean isMultiple() {
        return size() > 1;
    }

    /**
     * Get the number of segments in the path.
     *
     * @return the size of the path; never negative
     */
    int size();

    /**
     * Get the optional parent path.
     *
     * @return an optional containing the parent (if this is not the root path), or an empty optional if this is the root path.
     */
    Optional<Path> parent();

    /**
     * Get the last segment, if there is one.
     *
     * @return an optional containing the last segment of this path (if this is not the root path), or an empty optional if this
     *         is the root path.
     */
    Optional<String> lastSegment();

    /**
     * Get a portion of this path that has a specified number of segments.
     * @param length the number of segments
     * @return the subpath, or this path if the supplied length is equal to {@code this.size()}
     */
    Path subpath(int length);

    /**
     * Get the segment at the given index.
     * @param index the index of the segment
     * @return the segment
     * @throws IllegalArgumentException if the index value is equal to or greater than #size()
     */
    String segment(int index);

    /**
     * Create a new path consisting of this path with one or more additional segments given by the relative path.
     * @param relPath the relative path to be appended to this path; may not be null
     * @return the new path
     */
    default Path append(String relPath) {
        return append(Path.parse(relPath));
    }

    /**
     * Create a new path consisting of this path appended with the given path that will be treated as a relative path.
     * @param relPath the relative path to be appended to this path; may not be null
     * @return the new path
     */
    Path append(Path relPath);

    /**
     * Obtain the representation of this path as a relative path without the leading '/'.
     * @return the relative path; never null but may be empty
     */
    String toRelativePath();

    /**
     * Call the consumer with the path of every ancestor (except root) down to this path.
     *
     * @param consumer the function to call on each path segment
     */
    default void fromRoot(Consumer<Path> consumer) {
        Path path = root();
        for (String segment : this) {
            path = path.append(segment);
            consumer.accept(path);
        }
    }
}
