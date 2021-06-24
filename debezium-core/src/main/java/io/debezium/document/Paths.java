/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import io.debezium.annotation.Immutable;
import io.debezium.util.HashCode;
import io.debezium.util.Iterators;
import io.debezium.util.Joiner;
import io.debezium.util.Strings;

/**
 * A package-level utility that implements useful operations to create paths.
 *
 * @author Randall Hauch
 */
@Immutable
final class Paths {

    private static final Pattern PATH_SEPARATOR_PATTERN = Pattern.compile("/");

    static Path parse(String path, boolean resolveJsonPointerEscapes) {
        // Remove leading and trailing whitespace and '/' characters ...
        path = Strings.trim(path, (c) -> c < ' ' || c == '/');
        if (path.length() == 0) {
            return RootPath.INSTANCE;
        }
        String[] segments = PATH_SEPARATOR_PATTERN.split(path);
        if (segments.length == 1) {
            return new SingleSegmentPath(parseSegment(segments[0], resolveJsonPointerEscapes));
        }
        if (resolveJsonPointerEscapes) {
            for (int i = 0; i != segments.length; ++i) {
                segments[i] = parseSegment(segments[i], true);
            }
        }
        return new MultiSegmentPath(segments);
    }

    private static String parseSegment(String segment, boolean resolveJsonPointerEscapes) {
        if (resolveJsonPointerEscapes) {
            segment = segment.replaceAll("\\~1", "/").replaceAll("\\~0", "~");
        }
        return segment;
    }

    static interface InnerPath {
        int copyInto(String[] segments, int start);
    }

    static final class RootPath implements Path, InnerPath {

        public static final Path INSTANCE = new RootPath();
        public static final Optional<Path> OPTIONAL_OF_ROOT = Optional.of(RootPath.INSTANCE);

        private RootPath() {
        }

        @Override
        public Optional<Path> parent() {
            return Optional.empty();
        }

        @Override
        public Optional<String> lastSegment() {
            return Optional.empty();
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public int hashCode() {
            return 1;
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this;
        }

        @Override
        public String toString() {
            return "/";
        }

        @Override
        public String toRelativePath() {
            return "";
        }

        @Override
        public Iterator<String> iterator() {
            return Iterators.empty();
        }

        @Override
        public void forEach(Consumer<? super String> consumer) {
        }

        @Override
        public Path subpath(int length) {
            if (length != 0) {
                throw new IllegalArgumentException("Invalid subpath length: " + length);
            }
            return this;
        }

        @Override
        public String segment(int index) {
            throw new IllegalArgumentException("Invalid segment index: " + index);
        }

        @Override
        public Path append(Path relPath) {
            return relPath;
        }

        @Override
        public int copyInto(String[] segments, int start) {
            return 0;
        }
    }

    static final class SingleSegmentPath implements Path, InnerPath {
        private final Optional<String> segment;

        protected SingleSegmentPath(String segment) {
            assert segment != null;
            this.segment = Optional.of(segment); // wrap because we're always giving it away
        }

        @Override
        public Optional<Path> parent() {
            return Path.optionalRoot();
        }

        @Override
        public Optional<String> lastSegment() {
            return segment;
        }

        @Override
        public int size() {
            return 1;
        }

        @Override
        public int hashCode() {
            return segment.get().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof Path) {
                Path that = (Path) obj;
                if (this.size() != that.size()) {
                    return false;
                }
                return this.lastSegment().get().equals(that.lastSegment().get());
            }
            return false;
        }

        @Override
        public String toString() {
            return "/" + segment.get();
        }

        @Override
        public String toRelativePath() {
            return segment.get();
        }

        @Override
        public Iterator<String> iterator() {
            return Iterators.with(segment.get());
        }

        @Override
        public void forEach(Consumer<? super String> consumer) {
            consumer.accept(segment.get());
        }

        @Override
        public Path subpath(int length) {
            if (length > size() || length < 0) {
                throw new IllegalArgumentException("Invalid subpath length: " + length);
            }
            return length == 1 ? this : Path.root();
        }

        @Override
        public String segment(int index) {
            if (index >= size() || index < 0) {
                throw new IllegalArgumentException("Invalid segment index: " + index);
            }
            return segment.get();
        }

        @Override
        public Path append(Path relPath) {
            if (relPath.isRoot()) {
                return this;
            }
            if (relPath.isSingle()) {
                return new ChildPath(this, relPath.lastSegment().get());
            }
            String[] segments = new String[size() + relPath.size()];
            int offset = this.copyInto(segments, 0);
            copyPathInto(relPath, segments, offset);
            return new MultiSegmentPath(segments);
        }

        @Override
        public int copyInto(String[] segments, int start) {
            segments[start] = segment.get();
            return 1;
        }
    }

    static final class MultiSegmentPath implements Path, InnerPath {
        private final String[] segments;
        private final int hc;

        protected MultiSegmentPath(String[] segments) {
            this.segments = segments;
            assert size() > 1;
            this.hc = HashCode.compute(segments[0].hashCode(), segments[1].hashCode());
        }

        @Override
        public Optional<Path> parent() {
            if (size() == 2) {
                return Optional.of(new SingleSegmentPath(segments[0]));
            }
            return Optional.of(new MultiSegmentPath(Arrays.copyOf(segments, segments.length - 1)));
        }

        @Override
        public Optional<String> lastSegment() {
            return Optional.of(segments[segments.length - 1]);
        }

        @Override
        public int size() {
            return segments.length;
        }

        @Override
        public int hashCode() {
            return hc;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof Path) {
                Path that = (Path) obj;
                if (this.size() != that.size()) {
                    return false;
                }
                Iterator<String> thisIter = this.iterator();
                Iterator<String> thatIter = that.iterator();
                while (thisIter.hasNext()) {
                    if (!thisIter.next().equals(thatIter.next())) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            return Joiner.on("/", "/").join(segments);
        }

        @Override
        public String toRelativePath() {
            return Joiner.on("", "/").join(segments);
        }

        @Override
        public Iterator<String> iterator() {
            return Iterators.with(segments);
        }

        @Override
        public void forEach(Consumer<? super String> consumer) {
            for (String segment : segments) {
                consumer.accept(segment);
            }
        }

        @Override
        public Path subpath(int length) {
            if (length > size() || length < 0) {
                throw new IllegalArgumentException("Invalid subpath length: " + length);
            }
            if (length == 0) {
                return RootPath.INSTANCE;
            }
            if (length == 1) {
                return new SingleSegmentPath(segments[0]);
            }
            if (length == size()) {
                return this;
            }
            return new MultiSegmentPath(Arrays.copyOf(segments, length));
        }

        @Override
        public String segment(int index) {
            if (index >= size() || index < 0) {
                throw new IllegalArgumentException("Invalid segment index: " + index);
            }
            return segments[index];
        }

        @Override
        public Path append(Path relPath) {
            if (relPath.isRoot()) {
                return this;
            }
            if (relPath.isSingle()) {
                return new ChildPath(this, relPath.lastSegment().get());
            }
            String[] segments = new String[size() + relPath.size()];
            int offset = this.copyInto(segments, 0);
            copyPathInto(relPath, segments, offset);
            return new MultiSegmentPath(segments);
        }

        @Override
        public int copyInto(String[] segments, int start) {
            System.arraycopy(this.segments, 0, segments, start, this.segments.length);
            return this.segments.length;
        }
    }

    static final class ChildPath implements Path, InnerPath {
        private final Path parent;
        private final String segment;

        protected ChildPath(Path parent, String segment) {
            assert parent instanceof InnerPath;
            this.parent = parent;
            this.segment = segment;
        }

        @Override
        public Iterator<String> iterator() {
            return Iterators.join(parent, segment);
        }

        @Override
        public Optional<String> lastSegment() {
            return Optional.of(segment);
        }

        @Override
        public Optional<Path> parent() {
            return Optional.of(parent);
        }

        @Override
        public int size() {
            return parent.size() + 1;
        }

        @Override
        public int hashCode() {
            return HashCode.compute(parent, segment);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof Path) {
                Path that = (Path) obj;
                if (this.size() != that.size()) {
                    return false;
                }
                if (!this.parent.equals(that.parent())) {
                    return false;
                }
                return this.segment.equals(that.lastSegment().get());
            }
            return false;
        }

        @Override
        public String toString() {
            return Joiner.on("/", "/").join(parent.toString(), segment);
        }

        @Override
        public String toRelativePath() {
            return Joiner.on("/").join(parent.toRelativePath(), segment);
        }

        @Override
        public String segment(int index) {
            if (index >= size() || index < 0) {
                throw new IllegalArgumentException("Invalid segment index: " + index);
            }
            return index < parent.size() ? parent.segment(index) : segment;
        }

        @Override
        public Path subpath(int length) {
            if (length > size() || length < 0) {
                throw new IllegalArgumentException("Invalid subpath length: " + length);
            }
            return length <= parent.size() ? parent.subpath(length) : this;
        }

        @Override
        public Path append(Path relPath) {
            if (relPath.isRoot()) {
                return this;
            }
            if (relPath.isSingle()) {
                return new ChildPath(this, relPath.lastSegment().get());
            }
            String[] segments = new String[size() + relPath.size() + 1];
            int offset = copyInto(segments, 0);
            copyPathInto(relPath, segments, offset);
            return new MultiSegmentPath(segments);
        }

        @Override
        public int copyInto(String[] segments, int start) {
            int copied = ((InnerPath) parent).copyInto(segments, start);
            segments[copied] = this.segment;
            return size();
        }

    }

    static int copyPathInto(Path path, String[] segments, int start) {
        if (path instanceof InnerPath) {
            return ((InnerPath) path).copyInto(segments, start);
        }
        int i = start;
        for (String segment : path) {
            segments[i++] = segment;
        }
        return i;
    }

    private Paths() {
    }

}
