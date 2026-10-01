/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

public abstract class Segments<S extends Slot> implements Iterable<S> {
    private final S nullSlot = newSlot();
    private final List<S> slots = new ArrayList<>();
    private final S[][] segments = newSegments(65536);
    private int segmentSize;
    private int modCount;

    public Segments() {
        this(1024, 34);
    }

    public Segments(int initialSegmentCount, int initialSegmentSize) {
        for (int usn = 0; usn < initialSegmentCount; usn++) {
            segments[usn] = newSlots(newSegment(initialSegmentSize), 0);
        }
        segmentSize = initialSegmentSize;
    }

    protected abstract S[][] newSegments(int size);

    protected abstract S[] newSegment(int size);

    protected abstract S newSlot();

    private S[] newSlots(S[] segment, int i) {
        while (i < segment.length) {
            segment[i++] = newSlot();
        }
        return segment;
    }

    private S getOrCreate(long xid) {
        int sltUsn = (int) Long.reverseBytes(xid);
        if (sltUsn == 0xffffffff) {
            return nullSlot;
        }

        int slt = sltUsn >>> 16;
        int usn = sltUsn & 0xffff;
        S[] segment = segments[usn];
        if (segment == null || segment.length <= slt) {
            if (segmentSize <= slt) {
                segmentSize = slt + 1;
            }
            segment = segment == null ? newSlots(newSegment(segmentSize), 0)
                    : newSlots(Arrays.copyOf(segment, segmentSize), segment.length);
            segments[usn] = segment;
        }

        return segment[slt];
    }

    public S get(long xid) {
        S slot = getOrCreate(xid);
        if (slot.occupied()) {
            int sqn = (int) xid;
            if (sqn == slot.sqn || sqn == 0xffffffff) {
                return slot;
            }
            throw new IllegalStateException("Invalid XID %016x: The slot is occupied by the XID %016x".formatted(
                    xid, xid & 0xffffffff00000000L | slot.sqn & 0xffffffffL));
        }

        return slot;
    }

    @Override
    public Iterator<S> iterator() {
        return new Itr();
    }

    public Stream<S> stream() {
        return slots.stream();
    }

    public int size() {
        return slots.size();
    }

    public boolean isEmpty() {
        return slots.isEmpty();
    }

    public S occupy(long xid) {
        int sqn = (int) xid;
        if (sqn == 0xffffffff) {
            throw new IllegalArgumentException("Invalid XID %016x: XIDSQN is required to occupy a slot".formatted(xid));
        }

        S slot = getOrCreate(xid);
        if (slot.occupied()) {
            if (sqn == slot.sqn) {
                return slot;
            }
            throw new IllegalStateException("Invalid XID %016x: The slot is occupied by the XID %016x".formatted(
                    xid, xid & 0xffffffff00000000L | slot.sqn & 0xffffffffL));
        }

        slot.sqn = sqn;
        slot.index = slots.size();
        slots.add(slot);
        modCount++;
        return slot;
    }

    public S vacate(long xid) {
        return vacate(get(xid));
    }

    private S vacate(S slot) {
        if (!slot.occupied()) {
            return slot;
        }

        S last = slots.remove(slots.size() - 1);
        if (last != slot) {
            last.index = slot.index;
            slots.set(slot.index, last);
        }
        modCount++;

        slot.index = -1;
        return slot;
    }

    public void clear() {
        nullSlot.index = -1;
        nullSlot.clear();
        for (S slot : slots) {
            slot.index = -1;
            slot.clear();
        }
        slots.clear();
        modCount++;
    }

    private class Itr implements Iterator<S> {
        int cursor;
        int lastRet = -1;
        int expectedModCount = modCount;

        @Override
        public boolean hasNext() {
            return cursor != slots.size();
        }

        @Override
        public S next() {
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
            if (cursor >= slots.size()) {
                throw new NoSuchElementException();
            }
            return slots.get(lastRet = cursor++);
        }

        @Override
        public void remove() {
            if (lastRet < 0) {
                throw new IllegalStateException();
            }
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
            vacate(slots.get(cursor = lastRet));
            lastRet = -1;
            expectedModCount = modCount;
        }
    }
}
