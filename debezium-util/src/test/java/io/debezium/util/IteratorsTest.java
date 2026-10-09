/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.util.Iterators.PreviewIterator;

/**
 * Unit test for {@link Iterators}.
 */
class IteratorsTest {

    @Test
    public void emptyIteratorShouldHaveNoElements() {
        Iterator<String> iter = Iterators.empty();
        assertThat(iter.hasNext()).isFalse();
        assertThatThrownBy(iter::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void withValuesShouldIterateCorrectly() {
        Iterator<String> iter1 = Iterators.with("a");
        assertThat(iter1.hasNext()).isTrue();
        assertThat(iter1.next()).isEqualTo("a");
        assertThat(iter1.hasNext()).isFalse();

        Iterator<String> iter2 = Iterators.with("a", "b");
        assertThat(iter2.next()).isEqualTo("a");
        assertThat(iter2.next()).isEqualTo("b");
        assertThat(iter2.hasNext()).isFalse();

        Iterator<String> iter3 = Iterators.with("a", "b", "c");
        assertThat(iter3.next()).isEqualTo("a");
        assertThat(iter3.next()).isEqualTo("b");
        assertThat(iter3.next()).isEqualTo("c");
        assertThat(iter3.hasNext()).isFalse();

        Iterator<String> iter4 = Iterators.with("a", "b", "c", "d", "e");
        assertThat(iter4.next()).isEqualTo("a");
        assertThat(iter4.next()).isEqualTo("b");
        assertThat(iter4.next()).isEqualTo("c");
        assertThat(iter4.next()).isEqualTo("d");
        assertThat(iter4.next()).isEqualTo("e");
        assertThat(iter4.hasNext()).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2790")
    public void joinRemoveShouldOnlyRemoveFromActiveIterator() {
        List<String> list1 = new ArrayList<>(Arrays.asList("a", "b"));
        List<String> list2 = new ArrayList<>(Arrays.asList("c", "d"));
        Iterator<String> joined = Iterators.join(list1.iterator(), list2.iterator());

        // Remove from first iterator
        assertThat(joined.hasNext()).isTrue();
        assertThat(joined.next()).isEqualTo("a");
        joined.remove();
        assertThat(list1).containsExactly("b");
        assertThat(list2).containsExactly("c", "d");

        // Remove from second iterator
        assertThat(joined.next()).isEqualTo("b");
        assertThat(joined.next()).isEqualTo("c");
        joined.remove();
        assertThat(list1).containsExactly("b");
        assertThat(list2).containsExactly("d");

        assertThat(joined.next()).isEqualTo("d");
        assertThat(joined.hasNext()).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2790")
    public void joinRemoveShouldSupportRemoveAfterHasNextAtBoundary() {
        List<String> list1 = new ArrayList<>(Arrays.asList("a", "b"));
        List<String> list2 = new ArrayList<>(Arrays.asList("c", "d"));
        Iterator<String> joined = Iterators.join(list1.iterator(), list2.iterator());

        // next, next, hasNext, remove
        assertThat(joined.next()).isEqualTo("a");
        assertThat(joined.next()).isEqualTo("b");
        assertThat(joined.hasNext()).isTrue();
        joined.remove();

        assertThat(list1).containsExactly("a");
        assertThat(list2).containsExactly("c", "d");

        assertThat(joined.next()).isEqualTo("c");
        assertThat(joined.next()).isEqualTo("d");
        assertThat(joined.hasNext()).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2790")
    public void joinRemoveShouldThrowIllegalStateExceptionWhenNoCurrentElement() {
        List<String> list1 = new ArrayList<>(Arrays.asList("a"));
        List<String> list2 = new ArrayList<>(Arrays.asList("b"));
        Iterator<String> joined = Iterators.join(list1.iterator(), list2.iterator());

        assertThatThrownBy(joined::remove).isInstanceOf(IllegalStateException.class);

        assertThat(joined.next()).isEqualTo("a");
        joined.remove();
        assertThatThrownBy(joined::remove).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @FixFor("debezium/dbz#2790")
    public void aroundPairwiseShouldStopWhenFirstIsShorterThanSecond() {
        List<String> first = Arrays.asList("a", "b");
        List<Integer> second = Arrays.asList(1, 2, 3);
        Iterator<String> zipped = Iterators.around(first.iterator(), second.iterator(), (a, b) -> a + b);

        assertThat(zipped.hasNext()).isTrue();
        assertThat(zipped.next()).isEqualTo("a1");
        assertThat(zipped.hasNext()).isTrue();
        assertThat(zipped.next()).isEqualTo("b2");
        assertThat(zipped.hasNext()).isFalse();
        assertThatThrownBy(zipped::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    @FixFor("debezium/dbz#2790")
    public void aroundPairwiseShouldStopWhenSecondIsShorterThanFirst() {
        List<String> first = Arrays.asList("a", "b", "c");
        List<Integer> second = Arrays.asList(1, 2);
        Iterator<String> zipped = Iterators.around(first.iterator(), second.iterator(), (a, b) -> a + b);

        assertThat(zipped.hasNext()).isTrue();
        assertThat(zipped.next()).isEqualTo("a1");
        assertThat(zipped.hasNext()).isTrue();
        assertThat(zipped.next()).isEqualTo("b2");
        assertThat(zipped.hasNext()).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2790")
    public void previewShouldHandleNullValuesProperly() {
        List<String> list = Arrays.asList(null, "a", null, "b");
        PreviewIterator<String> preview = Iterators.preview(list.iterator());

        // Peek null element
        assertThat(preview.hasNext()).isTrue();
        assertThat(preview.peek()).isNull();
        assertThat(preview.peek()).isNull(); // Repeated peek
        assertThat(preview.hasNext()).isTrue();
        assertThat(preview.next()).isNull();

        // Peek non-null element
        assertThat(preview.hasNext()).isTrue();
        assertThat(preview.peek()).isEqualTo("a");
        assertThat(preview.next()).isEqualTo("a");

        // Peek second null element
        assertThat(preview.hasNext()).isTrue();
        assertThat(preview.peek()).isNull();
        assertThat(preview.next()).isNull();

        // Next without peeking
        assertThat(preview.hasNext()).isTrue();
        assertThat(preview.next()).isEqualTo("b");

        // Exhausted
        assertThat(preview.hasNext()).isFalse();
        assertThat(preview.peek()).isNull();
        assertThatThrownBy(preview::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    @FixFor("debezium/dbz#2790")
    public void previewSingleNullElement() {
        List<String> list = Collections.singletonList((String) null);
        PreviewIterator<String> preview = Iterators.preview(list.iterator());

        assertThat(preview.peek()).isNull();
        assertThat(preview.hasNext()).isTrue();
        assertThat(preview.next()).isNull();
        assertThat(preview.hasNext()).isFalse();
    }

    @Test
    public void transformShouldTransformElements() {
        List<String> list = Arrays.asList("1", "2", "3");
        Iterator<Integer> transformed = Iterators.transform(list.iterator(), Integer::parseInt);

        assertThat(transformed.hasNext()).isTrue();
        assertThat(transformed.next()).isEqualTo(1);
        assertThat(transformed.next()).isEqualTo(2);
        assertThat(transformed.next()).isEqualTo(3);
        assertThat(transformed.hasNext()).isFalse();
    }

    @Test
    public void readOnlyShouldNotAllowModifications() {
        List<String> list = new ArrayList<>(Arrays.asList("a", "b"));
        Iterator<String> readOnly = Iterators.readOnly(list.iterator());

        assertThat(readOnly.next()).isEqualTo("a");
        assertThatThrownBy(readOnly::remove).isInstanceOf(UnsupportedOperationException.class);
    }
}
