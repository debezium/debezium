/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.debezium.annotation.Immutable;

/**
 * A utility for creating iterators.
 *
 * @author Randall Hauch
 */
@Immutable
public class Iterators {

    public static <T> Iterator<T> empty() {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public T next() {
                throw new NoSuchElementException();
            }
        };
    }

    public static <T> Iterator<T> with(final T value) {
        return new Iterator<T>() {
            private boolean finished = false;

            @Override
            public boolean hasNext() {
                return !finished;
            }

            @Override
            public T next() {
                if (finished) {
                    throw new NoSuchElementException();
                }
                finished = true;
                return value;
            }
        };
    }

    public static <T> Iterator<T> with(T value1, T value2) {
        return new Iterator<T>() {
            private int remaining = 2;

            @Override
            public boolean hasNext() {
                return remaining > 0;
            }

            @Override
            public T next() {
                if (remaining == 2) {
                    --remaining;
                    return value1;
                }
                if (remaining == 1) {
                    --remaining;
                    return value2;
                }
                throw new NoSuchElementException();
            }
        };
    }

    public static <T> Iterator<T> with(T value1, T value2, T value3) {
        return new Iterator<T>() {
            private int remaining = 3;

            @Override
            public boolean hasNext() {
                return remaining > 0;
            }

            @Override
            public T next() {
                if (remaining == 3) {
                    --remaining;
                    return value1;
                }
                if (remaining == 2) {
                    --remaining;
                    return value2;
                }
                if (remaining == 1) {
                    --remaining;
                    return value3;
                }
                throw new NoSuchElementException();
            }
        };
    }

    @SafeVarargs
    public static <T> Iterator<T> with(T value1, T value2, T value3, T... additional) {
        return new Iterator<T>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < additional.length + 3;
            }

            @Override
            public T next() {
                try {
                    if (index == 0) {
                        return value1;
                    }
                    if (index == 1) {
                        return value2;
                    }
                    if (index == 2) {
                        return value3;
                    }
                    if (index < additional.length + 3) {
                        return additional[index - 3];
                    }
                    --index;
                    throw new NoSuchElementException();
                }
                finally {
                    ++index;
                }
            }
        };
    }

    public static <T> Iterator<T> with(T[] values) {
        return new Iterator<T>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < values.length;
            }

            @Override
            public T next() {
                try {
                    if (index < values.length) {
                        return values[index];
                    }
                    --index;
                    throw new NoSuchElementException();
                }
                finally {
                    ++index;
                }
            }
        };
    }

    public static <T, U, V> Iterator<V> around(Iterable<? extends T> first,
                                               Iterable<? extends U> second,
                                               BiFunction<T, U, V> conversion) {
        return around(first.iterator(), second.iterator(), conversion);
    }

    public static <T, U, V> Iterator<V> around(final Iterator<? extends T> first,
                                               final Iterator<? extends U> second,
                                               final BiFunction<T, U, V> combineFirstAndSecond) {
        return new Iterator<V>() {
            @Override
            public boolean hasNext() {
                return second.hasNext();
            }

            @Override
            public V next() {
                return combineFirstAndSecond.apply(first.next(), second.next());
            }
        };
    }

    public static <V, T> Iterator<T> around(final Iterable<? extends V> iterable, Function<V, T> conversion) {
        return around(iterable.iterator(), conversion);
    }

    public static <V, T> Iterator<T> around(final Iterator<? extends V> iterator, Function<V, T> conversion) {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                return conversion.apply(iterator.next());
            }

            @Override
            public void remove() {
                iterator.remove();
            }
        };
    }

    public static <T> Iterable<T> around(final Iterator<T> iterator) {
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return iterator;
            }
        };
    }

    public static <T> Iterator<T> readOnly(final Iterator<T> iterator) {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                return iterator.next();
            }
        };
    }

    public static <V, T> Iterator<T> readOnly(final Iterator<? extends V> iterator, Function<V, T> conversion) {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                return conversion.apply(iterator.next());
            }
        };
    }

    public static <T> Iterator<T> readOnly(final Iterable<T> iterable) {
        return readOnly(iterable.iterator());
    }

    public static <V, T> Iterator<T> readOnly(final Iterable<V> iterable, Function<V, T> conversion) {
        return readOnly(iterable.iterator(), conversion);
    }

    public static <T> Iterable<T> readOnlyIterable(final Iterable<T> iterable) {
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return readOnly(iterable.iterator());
            }
        };
    }

    public static <V, T> Iterable<T> readOnlyIterable(final Iterable<? extends V> iterable, Function<V, T> conversion) {
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return readOnly(iterable.iterator(), conversion);
            }
        };
    }

    public static <T> Iterator<T> join(Iterable<T> first, T last) {
        return join(first.iterator(), with(last));
    }

    public static <T> Iterator<T> join(Iterable<T> first, T last1, T last2) {
        return join(first.iterator(), with(last1, last2));
    }

    public static <T> Iterator<T> join(Iterable<T> first, T last1, T last2, T last3) {
        return join(first.iterator(), with(last1, last2, last3));
    }

    public static <T> Iterator<T> join(Iterable<T> first, T last1, T last2, T last3, T last4) {
        return join(first.iterator(), with(last1, last2, last3, last4));
    }

    public static <T> Iterator<T> join(Iterable<T> first, Iterable<T> second) {
        return join(first.iterator(), second.iterator());
    }

    public static <T> Iterator<T> join(Iterator<T> first, Iterator<T> second) {
        return new Iterator<T>() {
            private boolean completedFirst = false;

            @Override
            public boolean hasNext() {
                if (!completedFirst) {
                    if (first.hasNext()) {
                        return true;
                    }
                    completedFirst = true;
                }
                return second.hasNext();
            }

            @Override
            public T next() {
                if (!completedFirst) {
                    if (first.hasNext()) {
                        return first.next();
                    }
                    completedFirst = true;
                }
                return second.next();
            }

            @Override
            public void remove() {
                if (!completedFirst) {
                    first.remove();
                }
                second.remove();
            }
        };
    }

    /**
     * Get an {@link Iterable} from an {@link Iterator}.
     *
     * @param iterator the source iterator
     * @param <T> the iterator type
     *
     * @return the iterable
     */
    public static <T> Iterable<T> toIterable(Iterator<T> iterator) {
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return iterator;
            }
        };
    }

    /**
     * An iterator that is able to transform its contents to another type.
     *
     * @param <F> the source transform type
     * @param <T> the destination transform type
     */
    public interface TransformedIterator<F, T> extends Iterator<T> {
        T transform(F from);
    }

    /**
     * Transform an iterator from a given type to super types.
     *
     * @param fromIterator the source iterator
     * @param function the function to be applied when performing element transformation
     *
     * @param <F> the source transform type
     * @param <T> the destination transform type
     *
     * @return the transformed iterator
     */
    public static <F, T> Iterator<T> transform(Iterator<F> fromIterator, Function<? super F, ? extends T> function) {
        return new TransformedIterator<F, T>() {
            @Override
            public boolean hasNext() {
                return fromIterator.hasNext();
            }

            @Override
            public T next() {
                return transform(fromIterator.next());
            }

            @Override
            public void remove() {
                fromIterator.remove();
            }

            @Override
            public T transform(F from) {
                return function.apply(from);
            }
        };
    }

    /**
     * A read only iterator that is able to preview the next value without consuming it or altering the behavior or semantics
     * of the normal {@link Iterator} methods.
     *
     * @param <T> the type of value
     */
    public interface PreviewIterator<T> extends Iterator<T> {
        /**
         * Peek at the next value without consuming or using it. This method returns the same value if called multiple times
         * between {@link Iterator#next}.
         *
         * @return the next value, or null if there are no more
         */
        T peek();
    }

    /**
     * Get a read-only iterator that can peek at the next value before it is retrieved with {@link Iterator#next()}.
     *
     * @param iter the original iterator
     * @return the peeking iterator; may be null if {@code iter} is null
     */
    public static <T> PreviewIterator<T> preview(Iterator<T> iter) {
        if (iter == null) {
            return null;
        }
        if (iter instanceof PreviewIterator) {
            return (PreviewIterator<T>) iter;
        }
        return new PreviewIterator<T>() {
            private T nextValue;

            @Override
            public boolean hasNext() {
                return nextValue != null || iter.hasNext();
            }

            @Override
            public T next() {
                if (nextValue != null) {
                    T next = nextValue;
                    nextValue = null;
                    return next;
                }
                return iter.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public T peek() {
                if (nextValue != null) {
                    return nextValue;
                }
                if (iter.hasNext()) {
                    nextValue = iter.next();
                    return nextValue;
                }
                return null;
            }
        };
    }
}
