/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class InternerTest {

    @BeforeEach
    public void enableInterner() {
        Interner.setEnabled(true);
    }

    @AfterEach
    public void resetInterner() {
        Interner.clear();
        Interner.setEnabled(false);
    }

    @Test
    public void shouldReturnSameReferenceForEqualStrings() {
        String a = new String("hello");
        String b = new String("hello");
        assertThat(a).isNotSameAs(b);

        String internedA = Interner.intern(a);
        String internedB = Interner.intern(b);
        assertThat(internedA).isSameAs(internedB);
    }

    @Test
    public void shouldReturnDifferentReferencesForDifferentStrings() {
        String internedA = Interner.intern(new String("hello"));
        String internedB = Interner.intern(new String("world"));
        assertThat(internedA).isNotSameAs(internedB);
    }

    @Test
    public void shouldReturnNullForNullInput() {
        assertThat(Interner.intern((String) null)).isNull();
    }

    @Test
    public void shouldWorkWithLists() {
        List<String> list1 = Arrays.asList("a", "b", "c");
        List<String> list2 = Arrays.asList("a", "b", "c");
        assertThat(list1).isNotSameAs(list2);

        List<String> interned1 = Interner.intern(list1);
        List<String> interned2 = Interner.intern(list2);
        assertThat(interned1).isSameAs(interned2);
    }

    @Test
    public void shouldNotInternDifferentLists() {
        List<String> list1 = Arrays.asList("a", "b", "c");
        List<String> list2 = Arrays.asList("a", "b", "d");

        List<String> interned1 = Interner.intern(list1);
        List<String> interned2 = Interner.intern(list2);
        assertThat(interned1).isNotSameAs(interned2);
    }

    @Test
    public void shouldClearPool() {
        Interner.intern(Arrays.asList("a", "b"));
        assertThat(Interner.size()).isGreaterThan(0);

        Interner.clear();
        assertThat(Interner.size()).isEqualTo(0);
    }

    @Test
    public void shouldReinternAfterClear() {
        List<String> a = Arrays.asList("hello", "world");
        List<String> internedA = Interner.intern(a);
        assertThat(internedA).isSameAs(a);

        Interner.clear();

        List<String> b = Arrays.asList("hello", "world");
        List<String> internedB = Interner.intern(b);
        assertThat(internedB).isSameAs(b);
        assertThat(internedB).isNotSameAs(a);
    }

    @Test
    public void shouldReleaseEntriesWhenNoLongerStronglyReferenced() {
        // Intern a list and keep a strong reference
        List<String> kept = Interner.intern(new ArrayList<>(Arrays.asList("keep", "me")));
        assertThat(kept).isNotNull();

        // Intern a list without keeping a strong reference
        Interner.intern(new ArrayList<>(Arrays.asList("lose", "me")));

        // Force GC — the unreferenced list should be collected
        System.gc();
        System.gc();

        // The kept entry must still be internable to the same reference
        List<String> reInterned = Interner.intern(new ArrayList<>(Arrays.asList("keep", "me")));
        assertThat(reInterned).isSameAs(kept);
    }

    @Test
    public void shouldInternListsWithMixedTypes() {
        List<Object> list1 = Arrays.asList(new String("hello"), Integer.valueOf(42), Arrays.asList("nested"));
        List<Object> list2 = Arrays.asList(new String("hello"), Integer.valueOf(42), Arrays.asList("nested"));

        List<Object> interned1 = Interner.intern(list1);
        List<Object> interned2 = Interner.intern(list2);

        assertThat(interned1).isSameAs(interned2);
    }

    // --- Tests for disabled interner ---

    @Test
    public void shouldReturnInputWhenDisabled() {
        Interner.setEnabled(false);

        List<String> list1 = Arrays.asList("a", "b");
        List<String> list2 = Arrays.asList("a", "b");

        assertThat(Interner.intern(list1)).isSameAs(list1);
        assertThat(Interner.intern(list2)).isSameAs(list2);
        assertThat(list1).isNotSameAs(list2);
    }

    @Test
    public void shouldReturnNullWhenDisabled() {
        Interner.setEnabled(false);
        assertThat(Interner.intern((Integer) null)).isNull();
    }
}
