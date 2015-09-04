/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.harmony.java.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

/**
 * TreeMap is an implementation of SortedMap. All optional operations (adding
 * and removing) are supported. The values can be any objects. The keys can be
 * any objects which are comparable to each other either using their natural
 *
 * @param <K>
 *            type of key
 * @param <V>
 *            type of value
 *
 * @since 1.2
 */
public class TreeMap<K, V> extends AbstractMap<K, V> implements NavigableMap<K, V>, Cloneable, Serializable {
	private static final long serialVersionUID = 919286545866124006L;

	transient int size;

	transient Node<K, V> root;

	Comparator<? super K> comparator;

	transient int modCount;

	transient Set<Map.Entry<K, V>> entrySet;

	transient NavigableMap<K, V> descendingMap;

	transient NavigableSet<K> navigableKeySet;

	static class Node<K, V> implements Cloneable {
		static final int NODE_SIZE = Integer.getInteger(TreeMap.class.getName() + ".node_size", 64);

		Node<K, V> prev, next;
		Node<K, V> parent, left, right;
		V[] values;
		K[] keys;
		int left_idx = 0;
		int right_idx = -1;
		int size = 0;
		boolean color;

		@SuppressWarnings("unchecked")
		public Node() {
			keys = (K[]) new Object[NODE_SIZE];
			values = (V[]) new Object[NODE_SIZE];
		}

		@SuppressWarnings("unchecked")
		Node<K, V> clone(Node<K, V> parent) throws CloneNotSupportedException {
			Node<K, V> clone = (Node<K, V>) super.clone();
			clone.keys = keys.clone();
			clone.values = values.clone();
			clone.parent = parent;
			if (left != null) {
				clone.left = left.clone(clone);
			}
			if (right != null) {
				clone.right = right.clone(clone);
			}
			clone.prev = null;
			clone.next = null;
			return clone;
		}
	}

	/**
	 * Entry is an internal class which is used to hold the entries of a
	 * TreeMap.
	 *
	 * also used to record key, value, and position
	 */
	static class Entry<K, V> extends MapEntry<K, V> {
		Node<K, V> node;
		int index;

		Entry(Node<K, V> node, int index) {
			super(node.keys[index], node.values[index]);
			this.node = node;
			this.index = index;
		}

		@Override
		public V setValue(V object) {
			V result = value;
			value = object;
			this.node.values[index] = value;
			return result;
		}
	}

	private static abstract class AbstractSubMapIterator<K, V> {
		final NavigableSubMap<K, V> subMap;

		int expectedModCount;

		TreeMap.Node<K, V> node;

		TreeMap.Node<K, V> lastNode;

		TreeMap.Entry<K, V> boundaryPair;

		int offset;

		int lastOffset;

		AbstractSubMapIterator(final NavigableSubMap<K, V> map) {
			subMap = map;
			expectedModCount = subMap.m.modCount;
		}

		public void remove() {
			if (lastNode == null) {
				throw new IllegalStateException();
			}
			if (expectedModCount != subMap.m.modCount) {
				throw new ConcurrentModificationException();
			}

			Entry<K, V> entry;
			int idx = lastOffset;
			if (idx == lastNode.left_idx) {
				entry = subMap.m.removeLeftmost(lastNode, false);
			} else if (idx == lastNode.right_idx) {
				entry = subMap.m.removeRightmost(lastNode, false);
			} else {
				entry = subMap.m.removeMiddleElement(lastNode, idx, false);
			}
			if (entry != null) {
				node = entry.node;
				offset = entry.index;
				boundaryPair = getBoundaryNode();
			} else {
				node = null;
			}
			if (node != null && !this.subMap.isInRange(node.keys[offset])) {
				node = null;
			}
			lastNode = null;
			expectedModCount++;
		}

		abstract boolean hasNext();

		abstract TreeMap.Entry<K, V> getBoundaryNode();
	}

	private abstract static class AscendingSubMapIterator<K, V, T> extends AbstractSubMapIterator<K, V> implements Iterator<T> {

		AscendingSubMapIterator(NavigableSubMap<K, V> map) {
			super(map);
			TreeMap.Entry<K, V> entry = map.findStartNode();
			if (entry != null && map.checkUpperBound(entry.key)) {
				node = entry.node;
				offset = entry.index;
				boundaryPair = getBoundaryNode();
			}
		}

		@Override
		final TreeMap.Entry<K, V> getBoundaryNode() {
			if (subMap.toEnd) {
				return subMap.hiInclusive ? subMap.smallerOrEqualEntry(subMap.hi) : subMap.smallerEntry(subMap.hi);
			}
			return subMap.theBiggestEntry();
		}

		@Override
		public T next() {
			if (node == null) {
				throw new NoSuchElementException();
			}
			if (expectedModCount != subMap.m.modCount) {
				throw new ConcurrentModificationException();
			}

			lastNode = node;
			lastOffset = offset;
			if (offset != node.right_idx) {
				offset++;
			} else {
				node = node.next;
				if (node != null) {
					offset = node.left_idx;
				}
			}
//			boundaryPair = getBoundaryNode();
			if (boundaryPair != null && boundaryPair.node == lastNode && boundaryPair.index == lastOffset) {
				node = null;
			}
			return export(lastNode, lastOffset);
		}

		abstract T export(Node<K, V> node, int offset);

		@Override
		public final boolean hasNext() {
			return null != node;
		}
	}

	static class AscendingSubMapEntryIterator<K, V> extends AscendingSubMapIterator<K, V, Map.Entry<K, V>> {
		AscendingSubMapEntryIterator(NavigableSubMap<K, V> map) {
			super(map);
		}

		@Override
		Map.Entry<K, V> export(Node<K, V> node, int offset) {
			return newEntry(node, offset);
		}
	}

	static class AscendingSubMapKeyIterator<K, V> extends AscendingSubMapIterator<K, V, K> {
		AscendingSubMapKeyIterator(NavigableSubMap<K, V> map) {
			super(map);
		}

		@Override
		K export(Node<K, V> node, int offset) {
			return node.keys[offset];
		}
	}

	static class AscendingSubMapValueIterator<K, V> extends AscendingSubMapIterator<K, V, V> {
		AscendingSubMapValueIterator(NavigableSubMap<K, V> map) {
			super(map);
		}

		@Override
		V export(Node<K, V> node, int offset) {
			return node.values[offset];
		}
	}

	private abstract static class DescendingSubMapIterator<K, V, T> extends AbstractSubMapIterator<K, V> implements Iterator<T> {

		DescendingSubMapIterator(NavigableSubMap<K, V> map) {
			super(map);
			TreeMap.Entry<K, V> entry;
			if (map.fromStart) {
				entry = map.loInclusive ? map.m.findFloorEntry(map.lo) : map.m.findLowerEntry(map.lo);
			} else {
				entry = map.m.findBiggestEntry();
			}
			if (entry != null) {
				if (!map.isInRange(entry.key)) {
					node = null;
					return;
				}
				node = entry.node;
				offset = entry.index;
			} else {
				node = null;
				return;
			}
			boundaryPair = getBoundaryNode();
			if (boundaryPair != null) {
				if (map.m.keyCompare(boundaryPair.key, entry.key) > 0) {
					node = null;
				}
			}
			if (map.toEnd && !map.hiInclusive) {
				// the last element may be the same with first one but it is not included
				if (map.m.keyCompare(map.hi, entry.key) == 0) {
					node = null;
				}
			}
		}

		@Override
		final TreeMap.Entry<K, V> getBoundaryNode() {
			if (subMap.toEnd) {
				return subMap.hiInclusive ? subMap.m.findCeilingEntry(subMap.hi) : subMap.m.findHigherEntry(subMap.hi);
			}
			return subMap.m.findSmallestEntry();
		}

		@Override
		public T next() {
			if (node == null) {
				throw new NoSuchElementException();
			}
			if (expectedModCount != subMap.m.modCount) {
				throw new ConcurrentModificationException();
			}

			lastNode = node;
			lastOffset = offset;
			if (offset != node.left_idx) {
				offset--;
			} else {
				node = node.prev;
				if (node != null) {
					offset = node.right_idx;
				}
			}
//			boundaryPair = getBoundaryNode();
			if (boundaryPair != null && boundaryPair.node == lastNode && boundaryPair.index == lastOffset) {
				node = null;
			}
			return export(lastNode, lastOffset);
		}

		abstract T export(Node<K, V> node, int offset);

		@Override
		public final boolean hasNext() {
			return node != null;
		}

		@Override
		public final void remove() {
			if (lastNode == null) {
				throw new IllegalStateException();
			}
			if (expectedModCount != subMap.m.modCount) {
				throw new ConcurrentModificationException();
			}

			Entry<K, V> entry;
			int idx = lastOffset;
			if (idx == lastNode.left_idx) {
				entry = subMap.m.removeLeftmost(lastNode, true);
			} else if (idx == lastNode.right_idx) {
				entry = subMap.m.removeRightmost(lastNode, true);
			} else {
				entry = subMap.m.removeMiddleElement(lastNode, idx, true);
			}
			if (entry != null) {
				node = entry.node;
				offset = entry.index;
				boundaryPair = getBoundaryNode();
			} else {
				node = null;
			}
			if (node != null && !this.subMap.isInRange(node.keys[offset])) {
				node = null;
			}
			lastNode = null;
			expectedModCount++;
		}
	}

	static class DescendingSubMapEntryIterator<K, V> extends DescendingSubMapIterator<K, V, Map.Entry<K, V>> {
		DescendingSubMapEntryIterator(NavigableSubMap<K, V> map) {
			super(map);
		}

		@Override
		Map.Entry<K, V> export(Node<K, V> node, int offset) {
			return newEntry(node, offset);
		}
	}

	static class DescendingSubMapKeyIterator<K, V> extends DescendingSubMapIterator<K, V, K> {
		DescendingSubMapKeyIterator(NavigableSubMap<K, V> map) {
			super(map);
		}

		@Override
		K export(Node<K, V> node, int offset) {
			return node.keys[offset];
		}
	}

	static class DescendingSubMapValueIterator<K, V> extends DescendingSubMapIterator<K, V, V> {
		DescendingSubMapValueIterator(NavigableSubMap<K, V> map) {
			super(map);
		}

		@Override
		V export(Node<K, V> node, int offset) {
			return node.values[offset];
		}
	}

	static class AscendingSubMapEntrySet<K, V> extends AbstractSet<Map.Entry<K, V>> {
		NavigableSubMap<K, V> map;

		AscendingSubMapEntrySet(NavigableSubMap<K, V> map) {
			this.map = map;
		}

		@Override
		public final Iterator<Map.Entry<K, V>> iterator() {
			return new AscendingSubMapEntryIterator<K, V>(map);
		}

		@Override
		public int size() {
			int size = 0;
			Iterator<Map.Entry<K, V>> it = new AscendingSubMapEntryIterator<K, V>(map);
			while (it.hasNext()) {
				it.next();
				size++;
			}
			return size;
		}
	}

	static class DescendingSubMapEntrySet<K, V> extends AbstractSet<Map.Entry<K, V>> {
		NavigableSubMap<K, V> map;

		DescendingSubMapEntrySet(NavigableSubMap<K, V> map) {
			this.map = map;
		}

		@Override
		public final Iterator<Map.Entry<K, V>> iterator() {
			return new DescendingSubMapEntryIterator<K, V>(map);
		}

		@Override
		public int size() {
			int size = 0;
			Iterator<Map.Entry<K, V>> it = new DescendingSubMapEntryIterator<K, V>(map);
			while (it.hasNext()) {
				it.next();
				size++;
			}
			return size;
		}
	}

	static class AscendingSubMapKeySet<K, V> extends AbstractSet<K> implements NavigableSet<K> {
		NavigableSubMap<K, V> map;

		AscendingSubMapKeySet(NavigableSubMap<K, V> map) {
			this.map = map;
		}

		@Override
		public final Iterator<K> iterator() {
			return new AscendingSubMapKeyIterator<K, V>(map);
		}

		@Override
		public final Iterator<K> descendingIterator() {
			return new DescendingSubMapKeyIterator<K, V>(map.descendingSubMap());
		}

		@Override
		public int size() {
			int size = 0;
			Iterator<Map.Entry<K, V>> it = new AscendingSubMapEntryIterator<K, V>(map);
			while (it.hasNext()) {
				it.next();
				size++;
			}
			return size;
		}

		@Override
		public K ceiling(K e) {
			Entry<K, V> ret = map.findCeilingEntry(e);
			if (ret != null && map.isInRange(ret.key)) {
				return ret.key;
			} else {
				return null;
			}
		}

		@Override
		public NavigableSet<K> descendingSet() {
			return new DescendingSubMapKeySet<K, V>(map.descendingSubMap());
		}

		@Override
		public K floor(K e) {
			Entry<K, V> ret = map.findFloorEntry(e);
			if (ret != null && map.isInRange(ret.key)) {
				return ret.key;
			} else {
				return null;
			}
		}

		@Override
		public NavigableSet<K> headSet(K end, boolean endInclusive) {
			boolean isInRange = true;
			int result;
			if (map.toEnd) {
				result = (null != comparator()) ? comparator().compare(end, map.hi) : toComparable(end).compareTo(map.hi);
				isInRange = (map.hiInclusive || !endInclusive) ? result <= 0 : result < 0;
			}
			if (map.fromStart) {
				result = (null != comparator()) ? comparator().compare(end, map.lo) : toComparable(end).compareTo(map.lo);
				isInRange = isInRange && ((map.loInclusive || !endInclusive) ? result >= 0 : result > 0);
			}
			if (isInRange) {
				if (map.fromStart) {
					return new AscendingSubMapKeySet<K, V>(new AscendingSubMap<K, V>(map.lo, map.loInclusive, map.m, end, endInclusive));
				} else {
					return new AscendingSubMapKeySet<K, V>(new AscendingSubMap<K, V>(map.m, end, endInclusive));
				}
			}
			throw new IllegalArgumentException();
		}

		@Override
		public K higher(K e) {
			K ret = map.m.higherKey(e);
			if (ret != null && map.isInRange(ret)) {
				return ret;
			} else {
				return null;
			}
		}

		@Override
		public K lower(K e) {
			K ret = map.m.lowerKey(e);
			if (ret != null && map.isInRange(ret)) {
				return ret;
			} else {
				return null;
			}
		}

		@Override
		public K pollFirst() {
			Map.Entry<K, V> ret = map.firstEntry();
			if (ret == null) {
				return null;
			}
			map.m.remove(ret.getKey());
			return ret.getKey();
		}

		@Override
		public K pollLast() {
			Map.Entry<K, V> ret = map.lastEntry();
			if (ret == null) {
				return null;
			}
			map.m.remove(ret.getKey());
			return ret.getKey();
		}

		@Override
		public NavigableSet<K> subSet(K start, boolean startInclusive, K end, boolean endInclusive) {
			if (map.fromStart
					&& ((!map.loInclusive && startInclusive) ? map.m.keyCompare(start, map.lo) <= 0 : map.m.keyCompare(start, map.lo) < 0)
					|| (map.toEnd && ((!map.hiInclusive && (endInclusive || (startInclusive && start.equals(end)))) ? map.m.keyCompare(end, map.hi) >= 0
							: map.m.keyCompare(end, map.hi) > 0))) {
				throw new IllegalArgumentException();
			}
			if (map.m.keyCompare(start, end) > 0) {
				throw new IllegalArgumentException();
			}
			return new AscendingSubMapKeySet<K, V>(new AscendingSubMap<K, V>(start, startInclusive, map.m, end, endInclusive));
		}

		@Override
		public NavigableSet<K> tailSet(K start, boolean startInclusive) {
			boolean isInRange = true;
			int result;
			if (map.toEnd) {
				result = (null != comparator()) ? comparator().compare(start, map.hi) : toComparable(start).compareTo(map.hi);
				isInRange = (map.hiInclusive || !startInclusive) ? result <= 0 : result < 0;
			}
			if (map.fromStart) {
				result = (null != comparator()) ? comparator().compare(start, map.lo) : toComparable(start).compareTo(map.lo);
				isInRange = isInRange && ((map.loInclusive || !startInclusive) ? result >= 0 : result > 0);
			}

			if (isInRange) {
				if (map.toEnd) {
					return new AscendingSubMapKeySet<K, V>(new AscendingSubMap<K, V>(start, startInclusive, map.m, map.hi, map.hiInclusive));
				} else {
					return new AscendingSubMapKeySet<K, V>(new AscendingSubMap<K, V>(start, startInclusive, map.m));
				}
			}
			throw new IllegalArgumentException();
		}

		@Override
		public Comparator<? super K> comparator() {
			return map.m.comparator;
		}

		@Override
		public K first() {
			return map.firstKey();
		}

		@Override
		public SortedSet<K> headSet(K end) {
			return headSet(end, false);
		}

		@Override
		public K last() {
			return map.lastKey();
		}

		@Override
		public SortedSet<K> subSet(K start, K end) {
			return subSet(start, true, end, false);
		}

		@Override
		public SortedSet<K> tailSet(K start) {
			return tailSet(start, true);
		}

		@Override
		public boolean contains(Object object) {
			return map.containsKey(object);
		}

		@Override
		public boolean remove(Object object) {
			return this.map.remove(object) != null;
		}
	}

	static class DescendingSubMapKeySet<K, V> extends AbstractSet<K> implements NavigableSet<K> {
		NavigableSubMap<K, V> map;

		DescendingSubMapKeySet(NavigableSubMap<K, V> map) {
			this.map = map;
		}

		@Override
		public final Iterator<K> iterator() {
			return new DescendingSubMapKeyIterator<K, V>(map);
		}

		@Override
		public final Iterator<K> descendingIterator() {
			if (map.fromStart && map.toEnd) {
				return new AscendingSubMapKeyIterator<K, V>(new AscendingSubMap<K, V>(map.hi, map.hiInclusive, map.m, map.lo, map.loInclusive));
			}
			if (map.toEnd) {
				return new AscendingSubMapKeyIterator<K, V>(new AscendingSubMap<K, V>(map.hi, map.hiInclusive, map.m));
			}
			if (map.fromStart) {
				return new AscendingSubMapKeyIterator<K, V>(new AscendingSubMap<K, V>(map.m, map.lo, map.loInclusive));
			}
			return new AscendingSubMapKeyIterator<K, V>(new AscendingSubMap<K, V>(map.m));
		}

		@Override
		public int size() {
			int size = 0;
			Iterator<Map.Entry<K, V>> it = new DescendingSubMapEntryIterator<K, V>(map);
			while (it.hasNext()) {
				it.next();
				size++;
			}
			return size;
		}

		@Override
		public NavigableSet<K> descendingSet() {
			if (map.fromStart && map.toEnd) {
				return new AscendingSubMapKeySet<K, V>(new AscendingSubMap<K, V>(map.hi, map.hiInclusive, map.m, map.lo, map.loInclusive));
			}
			if (map.toEnd) {
				return new AscendingSubMapKeySet<K, V>(new AscendingSubMap<K, V>(map.hi, map.hiInclusive, map.m));
			}
			if (map.fromStart) {
				return new AscendingSubMapKeySet<K, V>(new AscendingSubMap<K, V>(map.m, map.lo, map.loInclusive));
			}
			return new AscendingSubMapKeySet<K, V>(new AscendingSubMap<K, V>(map.m));
		}

		@Override
		public K ceiling(K e) {
			Comparable<K> object = map.comparator() == null ? toComparable(e) : null;
			Entry<K, V> node = map.m.findFloorEntry(e);
			if (node != null && !map.checkUpperBound(node.key)) {
				return null;
			}

			if (node != null && !map.checkLowerBound(node.key)) {
				Entry<K, V> first = map.loInclusive ? map.m.findFloorEntry(map.lo) : map.m.findLowerEntry(map.lo);
				if (first != null && map.cmp(object, e, first.key) <= 0 && map.checkUpperBound(first.key)) {
					node = first;
				} else {
					node = null;
				}
			}
			return node == null ? null : node.key;
		}

		@Override
		public K floor(K e) {
			Entry<K, V> node = map.m.findCeilingEntry(e);
			if (node != null && !map.checkUpperBound(node.key)) {
				node = map.hiInclusive ? map.m.findCeilingEntry(map.hi) : map.m.findHigherEntry(map.hi);
			}

			if (node != null && !map.checkLowerBound(node.key)) {
				Comparable<K> object = map.comparator() == null ? toComparable(e) : null;
				Entry<K, V> first = map.loInclusive ? map.m.findFloorEntry(map.lo) : map.m.findLowerEntry(map.lo);
				if (first != null && map.cmp(object, e, first.key) > 0 && map.checkUpperBound(first.key)) {
					node = first;
				} else {
					node = null;
				}
			}
			return node == null ? null : node.key;
		}

		@Override
		public NavigableSet<K> headSet(K end, boolean endInclusive) {
			checkInRange(end, endInclusive);
			if (map.fromStart) {
				return new DescendingSubMapKeySet<K, V>(new DescendingSubMap<K, V>(map.lo, map.loInclusive, map.m, end, endInclusive));
			} else {
				return new DescendingSubMapKeySet<K, V>(new DescendingSubMap<K, V>(map.m, end, endInclusive));
			}
		}

		@Override
		public K higher(K e) {
			Comparable<K> object = map.comparator() == null ? toComparable(e) : null;
			Entry<K, V> node = map.m.findLowerEntry(e);
			if (node != null && !map.checkUpperBound(node.key)) {
				return null;
			}

			if (node != null && !map.checkLowerBound(node.key)) {
				Entry<K, V> first = map.loInclusive ? map.m.findFloorEntry(map.lo) : map.m.findLowerEntry(map.lo);
				if (first != null && map.cmp(object, e, first.key) < 0 && map.checkUpperBound(first.key)) {
					node = first;
				} else {
					node = null;
				}
			}
			return node == null ? null : node.key;
		}

		@Override
		public K lower(K e) {
			Entry<K, V> node = map.m.findHigherEntry(e);
			if (node != null && !map.checkUpperBound(node.key)) {
				node = map.hiInclusive ? map.m.findCeilingEntry(map.hi) : map.m.findHigherEntry(map.hi);
			}

			if (node != null && !map.checkLowerBound(node.key)) {
				Comparable<K> object = map.comparator() == null ? toComparable(e) : null;
				Entry<K, V> first = map.loInclusive ? map.m.findFloorEntry(map.lo) : map.m.findLowerEntry(map.lo);
				if (first != null && map.cmp(object, e, first.key) > 0 && map.checkUpperBound(first.key)) {
					node = first;
				} else {
					node = null;
				}
			}
			return node == null ? null : node.key;
		}

		@Override
		public K pollFirst() {
			Map.Entry<K, V> ret = map.firstEntry();
			if (ret == null) {
				return null;
			}
			map.m.remove(ret.getKey());
			return ret.getKey();
		}

		@Override
		public K pollLast() {
			Map.Entry<K, V> ret = map.lastEntry();
			if (ret == null) {
				return null;
			}
			map.m.remove(ret.getKey());
			return ret.getKey();
		}

		@Override
		public NavigableSet<K> subSet(K start, boolean startInclusive, K end, boolean endInclusive) {
			checkInRange(start, startInclusive);
			checkInRange(end, endInclusive);
			if ((null != map.comparator()) ? map.comparator().compare(start, end) > 0 : toComparable(start).compareTo(end) > 0) {
				throw new IllegalArgumentException();
			}
			return new DescendingSubMapKeySet<K, V>(new DescendingSubMap<K, V>(start, startInclusive, map.m, end, endInclusive));
		}

		@Override
		public NavigableSet<K> tailSet(K start, boolean startInclusive) {
			checkInRange(start, startInclusive);
			if (map.toEnd) {
				return new DescendingSubMapKeySet<K, V>(new DescendingSubMap<K, V>(start, startInclusive, map.m, map.hi, map.hiInclusive));
			} else {
				return new DescendingSubMapKeySet<K, V>(new DescendingSubMap<K, V>(start, startInclusive, map.m));
			}
		}

		void checkInRange(K key, boolean keyInclusive) {
			boolean isInRange = true;
			int result;
			if (map.toEnd) {
				result = (null != map.comparator()) ? map.comparator().compare(key, map.hi) : toComparable(key).compareTo(map.hi);
				isInRange = ((!map.hiInclusive) && keyInclusive) ? result < 0 : result <= 0;
			}
			if (map.fromStart) {
				result = (null != comparator()) ? comparator().compare(key, map.lo) : toComparable(key).compareTo(map.lo);
				isInRange = isInRange && (((!map.loInclusive) && keyInclusive) ? result > 0 : result >= 0);
			}
			if (!isInRange) {
				throw new IllegalArgumentException();
			}
		}

		@Override
		public Comparator<? super K> comparator() {
			return map.comparator();
		}

		@Override
		public K first() {
			return map.firstKey();
		}

		@Override
		public SortedSet<K> headSet(K end) {
			return headSet(end, false);
		}

		@Override
		public K last() {
			return map.lastKey();
		}

		@Override
		public SortedSet<K> subSet(K start, K end) {
			return subSet(start, true, end, false);
		}

		@Override
		public SortedSet<K> tailSet(K start) {
			return tailSet(start, true);
		}
	}

	static abstract class NavigableSubMap<K, V> extends AbstractMap<K, V> implements NavigableMap<K, V>, Serializable {

		private static final long serialVersionUID = -7141723745034997872L;

		final TreeMap<K, V> m;

		final K lo, hi;

		final boolean fromStart, toEnd;

		final boolean loInclusive, hiInclusive;

		NavigableSubMap(final K start, final boolean startKeyInclusive, final TreeMap<K, V> map, final K end, final boolean endKeyInclusive) {
			m = map;
			fromStart = toEnd = true;
			lo = start;
			hi = end;
			loInclusive = startKeyInclusive;
			hiInclusive = endKeyInclusive;
		}

		NavigableSubMap(final K start, final boolean startKeyInclusive, final TreeMap<K, V> map) {
			m = map;
			fromStart = true;
			toEnd = false;
			lo = start;
			hi = null;
			loInclusive = startKeyInclusive;
			hiInclusive = false;
		}

		NavigableSubMap(final TreeMap<K, V> map, final K end, final boolean endKeyInclusive) {
			m = map;
			fromStart = false;
			toEnd = true;
			lo = null;
			hi = end;
			loInclusive = false;
			hiInclusive = endKeyInclusive;
		}

		// the whole TreeMap
		NavigableSubMap(final TreeMap<K, V> map) {
			m = map;
			fromStart = toEnd = false;
			lo = hi = null;
			loInclusive = hiInclusive = false;
		}

		/*
		 * The basic public methods.
		 */

		@Override
		public Comparator<? super K> comparator() {
			return m.comparator();
		}

		@SuppressWarnings("unchecked")
		@Override
		public boolean containsKey(Object key) {
			checkNull(key);
			if (isInRange((K) key)) {
				return m.containsKey(key);
			}
			return false;
		}

		private void checkNull(Object key) {
			if (null == key && null == comparator()) {
				throw new NullPointerException();
			}
		}

		@Override
		public boolean isEmpty() {
			Iterator<K> it = this.keySet().iterator();
			if (it.hasNext()) {
				return false;
			}
			return true;
		}

		@Override
		public int size() {
			return entrySet().size();
		}

		@Override
		public V put(K key, V value) {
			checkNull(key);
			if (isInRange(key)) {
				return m.put(key, value);
			}
			throw new IllegalArgumentException();
		}

		@SuppressWarnings("unchecked")
		@Override
		public V get(Object key) {
			checkNull(key);
			if (isInRange((K) key)) {
				return m.get(key);
			}
			return null;
		}

		@SuppressWarnings("unchecked")
		@Override
		public V remove(Object key) {
			checkNull(key);
			if (isInRange((K) key)) {
				return m.remove(key);
			}
			return null;
		}

		/*
		 * The navigable methods.
		 */

		abstract NavigableSubMap<K, V> descendingSubMap();

		@Override
		public K firstKey() {
			Map.Entry<K, V> node = firstEntry();
			if (node != null) {
				return node.getKey();
			}
			throw new NoSuchElementException();
		}

		@Override
		public K lastKey() {
			Map.Entry<K, V> node = lastEntry();
			if (node != null) {
				return node.getKey();
			}
			throw new NoSuchElementException();
		}

		@Override
		public K higherKey(K key) {
			Map.Entry<K, V> entry = higherEntry(key);
			return (null == entry) ? null : entry.getKey();
		}

		@Override
		public K lowerKey(K key) {
			Map.Entry<K, V> entry = lowerEntry(key);
			return (null == entry) ? null : entry.getKey();
		}

		@Override
		public K ceilingKey(K key) {
			Map.Entry<K, V> entry = ceilingEntry(key);
			return (null == entry) ? null : entry.getKey();
		}

		@Override
		public K floorKey(K key) {
			Map.Entry<K, V> entry = floorEntry(key);
			return (null == entry) ? null : entry.getKey();
		}

		/*
		 * The sub-collection methods.
		 */

		@Override
		public Set<K> keySet() {
			return navigableKeySet();
		}

		@Override
		public NavigableSet<K> descendingKeySet() {
			return descendingMap().navigableKeySet();
		}

		@Override
		public SortedMap<K, V> subMap(K start, K end) {
			return subMap(start, true, end, false);
		}

		@Override
		public SortedMap<K, V> headMap(K end) {
			return headMap(end, false);
		}

		@Override
		public SortedMap<K, V> tailMap(K start) {
			return tailMap(start, true);
		}

		/**
		 * @return false if the key bigger than the end key (if any)
		 */
		final boolean checkUpperBound(K key) {
			if (toEnd) {
				int result = (null != comparator()) ? comparator().compare(key, hi) : toComparable(key).compareTo(hi);
				return hiInclusive ? result <= 0 : result < 0;
			}
			return true;
		}

		/**
		 * @return false if the key smaller than the start key (if any)
		 */
		final boolean checkLowerBound(K key) {
			if (fromStart) {
				int result = -((null != comparator()) ? comparator().compare(lo, key) : toComparable(lo).compareTo(key));
				return loInclusive ? result >= 0 : result > 0;
			}
			return true;
		}

		final boolean isInRange(K key) {
			return checkUpperBound(key) && checkLowerBound(key);
		}

		final TreeMap.Entry<K, V> theSmallestEntry() {
			TreeMap.Entry<K, V> result;
			if (!fromStart) {
				result = m.findSmallestEntry();
			} else {
				result = loInclusive ? m.findCeilingEntry(lo) : m.findHigherEntry(lo);
			}
			return (null != result && checkUpperBound(result.getKey())) ? result : null;
		}

		final TreeMap.Entry<K, V> theBiggestEntry() {
			TreeMap.Entry<K, V> result;
			if (!toEnd) {
				result = m.findBiggestEntry();
			} else {
				result = hiInclusive ? m.findFloorEntry(hi) : m.findLowerEntry(hi);
			}
			return (null != result && checkLowerBound(result.getKey())) ? result : null;
		}

		final TreeMap.Entry<K, V> smallerOrEqualEntry(K key) {
			TreeMap.Entry<K, V> result = findFloorEntry(key);
			return (null != result && checkLowerBound(result.getKey())) ? result : null;
		}

		private TreeMap.Entry<K, V> findFloorEntry(K key) {
			TreeMap.Entry<K, V> node = findFloorEntryImpl(key);

			if (node == null) {
				return null;
			}

			if (!checkUpperBound(node.key)) {
				node = findEndNode();
			}

			if (node != null && !checkLowerBound(node.key)) {
				Comparable<K> object = m.comparator == null ? toComparable(key) : null;
				if (cmp(object, key, this.lo) > 0) {
					node = findStartNode();
					if (node == null || cmp(object, key, node.key) < 0) {
						return null;
					}
				} else {
					node = null;
				}
			}
			return node;
		}

		private int cmp(Comparable<K> object, K key1, K key2) {
			return object != null ? object.compareTo(key2) : comparator().compare(key1, key2);
		}

		private TreeMap.Entry<K, V> findFloorEntryImpl(K key) {
			Comparable<K> object = comparator() == null ? toComparable(key) : null;
			K keyK = key;
			Node<K, V> node = this.m.root;
			Node<K, V> foundNode = null;
			int foundIndex = 0;
			while (node != null) {
				K[] keys = node.keys;
				int left_idx = node.left_idx;
				int result = cmp(object, keyK, keys[left_idx]);
				if (result < 0) {
					node = node.left;
				} else {
					foundNode = node;
					foundIndex = left_idx;
					if (result == 0) {
						break;
					}
					int right_idx = node.right_idx;
					if (left_idx != right_idx) {
						result = cmp(object, keyK, keys[right_idx]);
					}
					if (result >= 0) {
						foundNode = node;
						foundIndex = right_idx;
						if (result == 0) {
							break;
						}
						node = node.right;
					} else { /* search in node */
						int low = left_idx + 1, mid, high = right_idx - 1;
						while (low <= high && result != 0) {
							mid = (low + high) >> 1;
							result = cmp(object, keyK, keys[mid]);
							if (result >= 0) {
								foundNode = node;
								foundIndex = mid;
								low = mid + 1;
							} else {
								high = mid;
							}
							if (low == high && high == mid) {
								break;
							}
						}
						break;
					}
				}
			}
			if (foundNode != null && cmp(object, keyK, foundNode.keys[foundIndex]) < 0) {
				foundNode = null;
			}
			if (foundNode != null) {
				return newEntry(foundNode, foundIndex);
			}
			return null;
		}

		// find the node whose key equals startKey if any, or the next bigger
		// one than startKey if start exclusive
		private TreeMap.Entry<K, V> findStartNode() {
			if (fromStart) {
				if (loInclusive) {
					return m.findCeilingEntry(lo);
				} else {
					return m.findHigherEntry(lo);
				}
			} else {
				return theSmallestEntry();
			}
		}

		// find the node whose key equals endKey if any, or the next smaller
		// one than endKey if end exclusive
		private TreeMap.Entry<K, V> findEndNode() {
			if (hiInclusive) {
				return findFloorEntryImpl(hi);
			} else {
				return findLowerEntryImpl(hi);
			}
		}

		private TreeMap.Entry<K, V> findCeilingEntry(K key) {
			TreeMap.Entry<K, V> node = findCeilingEntryImpl(key);

			if (null == node) {
				return null;
			}

			if (!checkUpperBound(node.key)) {
				Comparable<K> object = m.comparator == null ? toComparable(key) : null;
				if (cmp(object, key, this.hi) < 0) {
					node = findEndNode();
					if (node != null && cmp(object, key, node.key) > 0) {
						return null;
					}
				} else {
					return null;
				}
			}

			if (node != null && !checkLowerBound(node.key)) {
				node = findStartNode();
			}

			return node;
		}

		private TreeMap.Entry<K, V> findLowerEntryImpl(K key) {
			Comparable<K> object = comparator() == null ? toComparable(key) : null;
			K keyK = key;
			Node<K, V> node = m.root;
			Node<K, V> foundNode = null;
			int foundIndex = 0;
			while (node != null) {
				K[] keys = node.keys;
				int left_idx = node.left_idx;
				int result = cmp(object, keyK, keys[left_idx]);
				if (result <= 0) {
					node = node.left;
				} else {
					foundNode = node;
					foundIndex = left_idx;
					int right_idx = node.right_idx;
					if (left_idx != right_idx) {
						result = cmp(object, key, keys[right_idx]);
					}
					if (result > 0) {
						foundNode = node;
						foundIndex = right_idx;
						node = node.right;
					} else { /* search in node */
						int low = left_idx + 1, mid, high = right_idx - 1;
						while (low <= high) {
							mid = (low + high) >> 1;
							result = cmp(object, key, keys[mid]);
							if (result > 0) {
								foundNode = node;
								foundIndex = mid;
								low = mid + 1;
							} else {
								high = mid;
							}
							if (low == high && high == mid) {
								break;
							}
						}
						break;
					}
				}
			}
			if (foundNode != null && cmp(object, keyK, foundNode.keys[foundIndex]) <= 0) {
				foundNode = null;
			}
			if (foundNode != null) {
				return newEntry(foundNode, foundIndex);
			}
			return null;
		}

		private TreeMap.Entry<K, V> findCeilingEntryImpl(K key) {
			Comparable<K> object = comparator() == null ? toComparable(key) : null;
			K keyK = key;
			Node<K, V> node = m.root;
			Node<K, V> foundNode = null;
			int foundIndex = 0;
			while (node != null) {
				K[] keys = node.keys;
				int left_idx = node.left_idx;
				int right_idx = node.right_idx;
				int result = cmp(object, keyK, keys[left_idx]);
				if (result < 0) {
					foundNode = node;
					foundIndex = left_idx;
					node = node.left;
				} else if (result == 0) {
					foundNode = node;
					foundIndex = left_idx;
					break;
				} else {
					if (left_idx != right_idx) {
						result = cmp(object, key, keys[right_idx]);
					}
					if (result > 0) {
						node = node.right;
					} else { /* search in node */
						foundNode = node;
						foundIndex = right_idx;
						if (result == 0) {
							break;
						}
						int low = left_idx + 1, mid, high = right_idx - 1;
						while (low <= high && result != 0) {
							mid = (low + high) >> 1;
							result = cmp(object, key, keys[mid]);
							if (result <= 0) {
								foundNode = node;
								foundIndex = mid;
								high = mid - 1;
							} else {
								low = mid + 1;
							}
							if (result == 0 || (low == high && high == mid)) {
								break;
							}
						}
						break;
					}
				}
			}
			if (foundNode != null && cmp(object, keyK, foundNode.keys[foundIndex]) > 0) {
				foundNode = null;
			}
			if (foundNode != null) {
				return newEntry(foundNode, foundIndex);
			}
			return null;
		}

		final TreeMap.Entry<K, V> smallerEntry(K key) {
			TreeMap.Entry<K, V> result = findLowerEntry(key);
			return (null != result && checkLowerBound(result.getKey())) ? result : null;
		}

		private TreeMap.Entry<K, V> findLowerEntry(K key) {
			TreeMap.Entry<K, V> node = findLowerEntryImpl(key);

			if (null == node) {
				return null;
			}

			if (!checkUpperBound(node.key)) {
				node = findEndNode();
			}

			if (!checkLowerBound(node.key)) {
				Comparable<K> object = m.comparator == null ? toComparable(key) : null;
				if (cmp(object, key, this.lo) > 0) {
					node = findStartNode();
					if (node == null || cmp(object, key, node.key) <= 0) {
						return null;
					}
				} else {
					node = null;
				}
			}

			return node;
		}

		private TreeMap.Entry<K, V> findHigherEntry(K key) {
			TreeMap.Entry<K, V> node = findHigherEntryImpl(key);

			if (node == null) {
				return null;
			}

			if (!checkUpperBound(node.key)) {
				Comparable<K> object = m.comparator == null ? toComparable(key) : null;
				if (cmp(object, key, this.hi) < 0) {
					node = findEndNode();
					if (node != null && cmp(object, key, node.key) >= 0) {
						return null;
					}
				} else {
					return null;
				}
			}

			if (node != null && !checkLowerBound(node.key)) {
				node = findStartNode();
			}

			return node;
		}

		TreeMap.Entry<K, V> findHigherEntryImpl(K key) {
			Comparable<K> object = m.comparator == null ? toComparable(key) : null;
			K keyK = key;
			Node<K, V> node = m.root;
			Node<K, V> foundNode = null;
			int foundIndex = 0;
			while (node != null) {
				K[] keys = node.keys;
				int right_idx = node.right_idx;
				int result = cmp(object, keyK, keys[right_idx]);
				if (result >= 0) {
					node = node.right;
				} else {
					foundNode = node;
					foundIndex = right_idx;
					int left_idx = node.left_idx;
					if (left_idx != right_idx) {
						result = cmp(object, key, keys[left_idx]);
					}
					if (result < 0) {
						foundNode = node;
						foundIndex = left_idx;
						node = node.left;
					} else { /* search in node */
						foundNode = node;
						foundIndex = right_idx;
						int low = left_idx + 1, mid, high = right_idx - 1;
						while (low <= high) {
							mid = (low + high) >> 1;
							result = cmp(object, key, keys[mid]);
							if (result < 0) {
								foundNode = node;
								foundIndex = mid;
								high = mid - 1;
							} else {
								low = mid + 1;
							}
							if (low == high && high == mid) {
								break;
							}
						}
						break;
					}
				}
			}
			if (foundNode != null && cmp(object, keyK, foundNode.keys[foundIndex]) >= 0) {
				foundNode = null;
			}
			if (foundNode != null) {
				return newEntry(foundNode, foundIndex);
			}
			return null;
		}
	}

	static class AscendingSubMap<K, V> extends NavigableSubMap<K, V> implements Serializable {
		private static final long serialVersionUID = 912986545866124060L;

		AscendingSubMap(K start, boolean startKeyInclusive, TreeMap<K, V> map, K end, boolean endKeyInclusive) {
			super(start, startKeyInclusive, map, end, endKeyInclusive);
		}

		AscendingSubMap(TreeMap<K, V> map, K end, boolean endKeyInclusive) {
			super(map, end, endKeyInclusive);
		}

		AscendingSubMap(K start, boolean startKeyInclusive, TreeMap<K, V> map) {
			super(start, startKeyInclusive, map);
		}

		AscendingSubMap(TreeMap<K, V> map) {
			super(map);
		}

		@Override
		public Map.Entry<K, V> firstEntry() {
			TreeMap.Entry<K, V> ret = theSmallestEntry();
			if (ret != null) {
				return newImmutableEntry(ret);
			} else {
				return null;
			}
		}

		@Override
		public Map.Entry<K, V> lastEntry() {
			TreeMap.Entry<K, V> ret = theBiggestEntry();
			if (ret != null) {
				return newImmutableEntry(ret);
			} else {
				return null;
			}
		}

		@Override
		public Map.Entry<K, V> pollFirstEntry() {
			TreeMap.Entry<K, V> node = theSmallestEntry();
			Map.Entry<K, V> result = newImmutableEntry(node);
			if (null != node) {
				m.remove(node.key);
			}
			return result;
		}

		@Override
		public Map.Entry<K, V> pollLastEntry() {
			TreeMap.Entry<K, V> node = theBiggestEntry();
			Map.Entry<K, V> result = newImmutableEntry(node);
			if (null != node) {
				m.remove(node.key);
			}
			return result;
		}

		@Override
		public Map.Entry<K, V> higherEntry(K key) {
			TreeMap.Entry<K, V> entry = super.findHigherEntry(key);
			if (null != entry && isInRange(entry.key)) {
				return newImmutableEntry(entry);
			} else {
				return null;
			}
		}

		@Override
		public Map.Entry<K, V> lowerEntry(K key) {
			TreeMap.Entry<K, V> entry = super.findLowerEntry(key);
			if (null != entry && isInRange(entry.key)) {
				return newImmutableEntry(entry);
			} else {
				return null;
			}
		}

		@Override
		public Map.Entry<K, V> ceilingEntry(K key) {
			TreeMap.Entry<K, V> entry = super.findCeilingEntry(key);
			if (null != entry && isInRange(entry.key)) {
				return newImmutableEntry(entry);
			} else {
				return null;
			}
		}

		@Override
		public Map.Entry<K, V> floorEntry(K key) {
			TreeMap.Entry<K, V> entry = super.findFloorEntry(key);
			if (null != entry && isInRange(entry.key)) {
				return newImmutableEntry(entry);
			} else {
				return null;
			}
		}

		@Override
		public Set<Map.Entry<K, V>> entrySet() {
			return new AscendingSubMapEntrySet<K, V>(this);
		}

		@Override
		public NavigableSet<K> navigableKeySet() {
			return new AscendingSubMapKeySet<K, V>(this);
		}

		@Override
		public NavigableMap<K, V> descendingMap() {
			if (fromStart && toEnd) {
				return new DescendingSubMap<K, V>(hi, hiInclusive, m, lo, loInclusive);
			}
			if (fromStart) {
				return new DescendingSubMap<K, V>(m, lo, loInclusive);
			}
			if (toEnd) {
				return new DescendingSubMap<K, V>(hi, hiInclusive, m);
			}
			return new DescendingSubMap<K, V>(m);
		}

		@Override
		NavigableSubMap<K, V> descendingSubMap() {
			if (fromStart && toEnd) {
				return new DescendingSubMap<K, V>(hi, hiInclusive, m, lo, loInclusive);
			}
			if (fromStart) {
				return new DescendingSubMap<K, V>(m, lo, loInclusive);
			}
			if (toEnd) {
				return new DescendingSubMap<K, V>(hi, hiInclusive, m);
			}
			return new DescendingSubMap<K, V>(m);
		}

		@Override
		public NavigableMap<K, V> subMap(K start, boolean startKeyInclusive, K end, boolean endKeyInclusive) {
			if (fromStart
					&& ((!loInclusive && startKeyInclusive) ? m.keyCompare(start, lo) <= 0 : m.keyCompare(start, lo) < 0)
					|| (toEnd && ((!hiInclusive && (endKeyInclusive || (startKeyInclusive && start.equals(end)))) ? m.keyCompare(end, hi) >= 0 : m.keyCompare(
							end, hi) > 0))) {
				throw new IllegalArgumentException();
			}
			if (m.keyCompare(start, end) > 0) {
				throw new IllegalArgumentException();
			}
			return new AscendingSubMap<K, V>(start, startKeyInclusive, m, end, endKeyInclusive);
		}

		@Override
		public NavigableMap<K, V> headMap(K end, boolean inclusive) {
			if (fromStart && ((!loInclusive && inclusive) ? m.keyCompare(end, lo) <= 0 : m.keyCompare(end, lo) < 0)) {
				throw new IllegalArgumentException();
			}
			if (toEnd && ((!hiInclusive && inclusive) ? m.keyCompare(end, hi) >= 0 : m.keyCompare(end, hi) > 0)) {
				throw new IllegalArgumentException();
			}
			if (checkUpperBound(end)) {
				if (this.fromStart) {
					return new AscendingSubMap<K, V>(this.lo, this.loInclusive, m, end, inclusive);
				}
				return new AscendingSubMap<K, V>(m, end, inclusive);
			} else {
				return this;
			}
		}

		@Override
		public NavigableMap<K, V> tailMap(K start, boolean inclusive) {
			if (fromStart && ((!loInclusive && inclusive) ? m.keyCompare(start, lo) <= 0 : m.keyCompare(start, lo) < 0)) {
				throw new IllegalArgumentException();
			}
			if (toEnd && ((!hiInclusive && inclusive) ? m.keyCompare(start, hi) >= 0 : m.keyCompare(start, hi) > 0)) {
				throw new IllegalArgumentException();
			}
			if (checkLowerBound(start)) {
				if (this.toEnd) {
					return new AscendingSubMap<K, V>(start, inclusive, m, this.hi, this.hiInclusive);
				}
				return new AscendingSubMap<K, V>(start, inclusive, m);
			} else {
				return this;
			}
		}

		@Override
		public Collection<V> values() {
			if (valuesCollection == null) {
				valuesCollection = new AscendingSubMapValuesCollection<K, V>(this);
			}
			return valuesCollection;
		}

		static class AscendingSubMapValuesCollection<K, V> extends AbstractCollection<V> {
			AscendingSubMap<K, V> subMap;

			public AscendingSubMapValuesCollection(AscendingSubMap<K, V> subMap) {
				this.subMap = subMap;
			}

			@Override
			public Iterator<V> iterator() {
				return new AscendingSubMapValueIterator<K, V>(subMap);
			}

			@Override
			public int size() {
				return subMap.size();
			}

			@Override
			public boolean isEmpty() {
				return subMap.isEmpty();
			}
		}
	}

	static class DescendingSubMap<K, V> extends NavigableSubMap<K, V> implements Serializable {
		private static final long serialVersionUID = 912986545866120460L;

		private final Comparator<? super K> reverseComparator = Collections.reverseOrder(m.comparator);

		DescendingSubMap(K start, boolean startKeyInclusive, TreeMap<K, V> map, K end, boolean endKeyInclusive) {
			super(start, startKeyInclusive, map, end, endKeyInclusive);
		}

		DescendingSubMap(K start, boolean startKeyInclusive, TreeMap<K, V> map) {
			super(start, startKeyInclusive, map);
		}

		DescendingSubMap(TreeMap<K, V> map, K end, boolean endKeyInclusive) {
			super(map, end, endKeyInclusive);
		}

		DescendingSubMap(TreeMap<K, V> map) {
			super(map);
		}

		@Override
		public Comparator<? super K> comparator() {
			return reverseComparator;
		}

		@Override
		public Map.Entry<K, V> firstEntry() {
			TreeMap.Entry<K, V> result;
			if (!fromStart) {
				result = m.findBiggestEntry();
			} else {
				result = loInclusive ? m.findFloorEntry(lo) : m.findLowerEntry(lo);
			}
			if (result == null || !isInRange(result.key)) {
				return null;
			}
			return newImmutableEntry(result);
		}

		@Override
		public Map.Entry<K, V> lastEntry() {
			TreeMap.Entry<K, V> result;
			if (!toEnd) {
				result = m.findSmallestEntry();
			} else {
				result = hiInclusive ? m.findCeilingEntry(hi) : m.findHigherEntry(hi);
			}
			if (result != null && !isInRange(result.key)) {
				return null;
			}
			return newImmutableEntry(result);
		}

		@Override
		public Map.Entry<K, V> pollFirstEntry() {
			TreeMap.Entry<K, V> node;
			if (fromStart) {
				node = loInclusive ? this.m.findFloorEntry(lo) : this.m.findLowerEntry(lo);
			} else {
				node = this.m.findBiggestEntry();
			}
			if (node != null && fromStart && (loInclusive ? this.m.keyCompare(lo, node.key) < 0 : this.m.keyCompare(lo, node.key) <= 0)) {
				node = null;
			}
			if (node != null && toEnd && (hiInclusive ? this.m.keyCompare(hi, node.key) > 0 : this.m.keyCompare(hi, node.key) >= 0)) {
				node = null;
			}
			Map.Entry<K, V> result = newImmutableEntry(node);
			if (null != node) {
				m.remove(node.key);
			}
			return result;
		}

		@Override
		public Map.Entry<K, V> pollLastEntry() {
			TreeMap.Entry<K, V> node;
			if (toEnd) {
				node = hiInclusive ? this.m.findCeilingEntry(hi) : this.m.findHigherEntry(hi);
			} else {
				node = this.m.findSmallestEntry();
			}
			if (node != null && fromStart && (loInclusive ? this.m.keyCompare(lo, node.key) < 0 : this.m.keyCompare(lo, node.key) <= 0)) {
				node = null;
			}
			if (node != null && toEnd && (hiInclusive ? this.m.keyCompare(hi, node.key) > 0 : this.m.keyCompare(hi, node.key) >= 0)) {
				node = null;
			}
			Map.Entry<K, V> result = newImmutableEntry(node);
			if (null != node) {
				m.remove(node.key);
			}
			return result;
		}

		@Override
		public Map.Entry<K, V> higherEntry(K key) {
			TreeMap.Entry<K, V> entry = this.m.findLowerEntry(key);
			if (null != entry && !checkLowerBound(entry.getKey())) {
				entry = loInclusive ? this.m.findFloorEntry(this.lo) : this.m.findLowerEntry(this.lo);
			}
			if (null != entry && !isInRange(entry.getKey())) {
				entry = null;
			}
			return newImmutableEntry(entry);
		}

		@Override
		public Map.Entry<K, V> lowerEntry(K key) {
			TreeMap.Entry<K, V> entry = this.m.findHigherEntry(key);
			if (null != entry && !checkUpperBound(entry.getKey())) {
				entry = hiInclusive ? this.m.findCeilingEntry(this.hi) : this.m.findHigherEntry(this.hi);
			}
			if (null != entry && !isInRange(entry.getKey())) {
				entry = null;
			}
			return newImmutableEntry(entry);
		}

		@Override
		public Map.Entry<K, V> ceilingEntry(K key) {
			Comparable<K> object = m.comparator == null ? toComparable(key) : null;
			TreeMap.Entry<K, V> entry;
			if (fromStart && m.cmp(object, key, lo) >= 0) {
				entry = loInclusive ? this.m.findFloorEntry(lo) : this.m.findLowerEntry(lo);
			} else {
				entry = this.m.findFloorEntry(key);
			}
			if (null != entry && !checkUpperBound(entry.getKey())) {
				entry = null;
			}
			return newImmutableEntry(entry);
		}

		@Override
		public Map.Entry<K, V> floorEntry(K key) {
			Comparable<K> object = m.comparator == null ? toComparable(key) : null;
			TreeMap.Entry<K, V> entry;
			if (toEnd && m.cmp(object, key, hi) <= 0) {
				entry = hiInclusive ? this.m.findCeilingEntry(hi) : this.m.findHigherEntry(hi);
			} else {
				entry = this.m.findCeilingEntry(key);
			}
			if (null != entry && !checkLowerBound(entry.getKey())) {
				entry = null;
			}
			return newImmutableEntry(entry);
		}

		@Override
		public Set<Map.Entry<K, V>> entrySet() {
			return new DescendingSubMapEntrySet<K, V>(this);
		}

		@Override
		public NavigableSet<K> navigableKeySet() {
			return new DescendingSubMapKeySet<K, V>(this);
		}

		@Override
		public NavigableMap<K, V> descendingMap() {
			if (fromStart && toEnd) {
				return new AscendingSubMap<K, V>(hi, hiInclusive, m, lo, loInclusive);
			}
			if (fromStart) {
				return new AscendingSubMap<K, V>(m, lo, loInclusive);
			}
			if (toEnd) {
				return new AscendingSubMap<K, V>(hi, hiInclusive, m);
			}
			return new AscendingSubMap<K, V>(m);
		}

		int keyCompare(K left, K right) {
			return (null != reverseComparator) ? reverseComparator.compare(left, right) : toComparable(left).compareTo(right);
		}

		@Override
		public NavigableMap<K, V> subMap(K start, boolean startKeyInclusive, K end, boolean endKeyInclusive) {
			// special judgement, the same reason as subMap(K,K)
			if (!checkUpperBound(start)) {
				throw new IllegalArgumentException();
			}
			if (fromStart
					&& ((!loInclusive && (startKeyInclusive || (endKeyInclusive && start.equals(end)))) ? keyCompare(start, lo) <= 0
							: keyCompare(start, lo) < 0)
					|| (toEnd && ((!hiInclusive && (endKeyInclusive)) ? keyCompare(end, hi) >= 0 : keyCompare(end, hi) > 0))) {
				throw new IllegalArgumentException();
			}
			if (keyCompare(start, end) > 0) {
				throw new IllegalArgumentException();
			}
			return new DescendingSubMap<K, V>(start, startKeyInclusive, m, end, endKeyInclusive);
		}

		@Override
		public NavigableMap<K, V> headMap(K end, boolean inclusive) {
			// check for error
			this.keyCompare(end, end);
			K inclusiveEnd = end; // inclusive ? end : m.higherKey(end);
			boolean isInRange = true;
			if (null != inclusiveEnd) {
				int result;
				if (toEnd) {
					result = (null != comparator()) ? comparator().compare(inclusiveEnd, hi) : toComparable(inclusiveEnd).compareTo(hi);
					isInRange = (hiInclusive || !inclusive) ? result <= 0 : result < 0;
				}
				if (fromStart) {
					result = (null != comparator()) ? comparator().compare(inclusiveEnd, lo) : toComparable(inclusiveEnd).compareTo(lo);
					isInRange = isInRange && ((loInclusive || !inclusive) ? result >= 0 : result > 0);
				}
			}
			if (isInRange) {
				if (this.fromStart) {
					return new DescendingSubMap<K, V>(this.lo, this.loInclusive, m, end, inclusive);
				}
				return new DescendingSubMap<K, V>(m, end, inclusive);
			}
			throw new IllegalArgumentException();
		}

		@Override
		public NavigableMap<K, V> tailMap(K start, boolean inclusive) {
			// check for error
			this.keyCompare(start, start);
			K inclusiveStart = start; // inclusive ? start : m.lowerKey(start);
			boolean isInRange = true;
			int result;
			if (null != inclusiveStart) {
				if (toEnd) {
					result = (null != comparator()) ? comparator().compare(inclusiveStart, hi) : toComparable(inclusiveStart).compareTo(hi);
					isInRange = (hiInclusive || !inclusive) ? result <= 0 : result < 0;
				}
				if (fromStart) {
					result = (null != comparator()) ? comparator().compare(inclusiveStart, lo) : toComparable(inclusiveStart).compareTo(lo);
					isInRange = isInRange && ((loInclusive || !inclusive) ? result >= 0 : result > 0);
				}
			}
			if (isInRange) {
				if (this.toEnd) {
					return new DescendingSubMap<K, V>(start, inclusive, m, this.hi, this.hiInclusive);
				}
				return new DescendingSubMap<K, V>(start, inclusive, m);

			}
			throw new IllegalArgumentException();
		}

		@Override
		public Collection<V> values() {
			if (valuesCollection == null) {
				valuesCollection = new DescendingSubMapValuesCollection<K, V>(this);
			}
			return valuesCollection;
		}

		static class DescendingSubMapValuesCollection<K, V> extends AbstractCollection<V> {
			DescendingSubMap<K, V> subMap;

			public DescendingSubMapValuesCollection(DescendingSubMap<K, V> subMap) {
				this.subMap = subMap;
			}

			@Override
			public boolean isEmpty() {
				return subMap.isEmpty();
			}

			@Override
			public Iterator<V> iterator() {
				return new DescendingSubMapValueIterator<K, V>(subMap);
			}

			@Override
			public int size() {
				return subMap.size();
			}
		}

		@Override
		NavigableSubMap<K, V> descendingSubMap() {
			if (fromStart && toEnd) {
				return new AscendingSubMap<K, V>(hi, hiInclusive, m, lo, loInclusive);
			}
			if (fromStart) {
				return new AscendingSubMap<K, V>(m, hi, hiInclusive);
			}
			if (toEnd) {
				return new AscendingSubMap<K, V>(lo, loInclusive, m);
			}
			return new AscendingSubMap<K, V>(m);
		}
	}

	/**
	 * Constructs a new empty {@code TreeMap} instance.
	 */
	public TreeMap() {
		super();
	}

	/**
	 * Constructs a new empty {@code TreeMap} instance with the specified
	 * comparator.
	 *
	 * @param comparator
	 *            the comparator to compare keys with.
	 */
	public TreeMap(Comparator<? super K> comparator) {
		this.comparator = comparator;
	}

	/**
	 * Constructs a new {@code TreeMap} instance containing the mappings from
	 * the specified map and using natural ordering.
	 *
	 * @param map
	 *            the mappings to add.
	 * @throws ClassCastException
	 *             if a key in the specified map does not implement the
	 *             Comparable interface, or if the keys in the map cannot be
	 *             compared.
	 */
	public TreeMap(Map<? extends K, ? extends V> map) {
		this();
		putAll(map);
	}

	/**
	 * Constructs a new {@code TreeMap} instance containing the mappings from
	 * the specified SortedMap and using the same comparator.
	 *
	 * @param map
	 *            the mappings to add.
	 */
	public TreeMap(SortedMap<K, ? extends V> map) {
		this(map.comparator());
		Node<K, V> lastNode = null;
		for (Map.Entry<K, ? extends V> entry : map.entrySet()) {
			lastNode = addToLast(lastNode, entry.getKey(), entry.getValue());
		}
	}

	Node<K, V> addToLast(Node<K, V> last, K key, V value) {
		if (last == null) {
			root = last = createNode(key, value);
			size = 1;
		} else if (last.size == Node.NODE_SIZE) {
			Node<K, V> newNode = createNode(key, value);
			attachToRight(last, newNode);
			balance(newNode);
			size++;
			last = newNode;
		} else {
			appendFromRight(last, key, value);
			size++;
		}
		return last;
	}

	/**
	 * Removes all mappings from this TreeMap, leaving it empty.
	 *
	 * @see Map#isEmpty()
	 * @see #size()
	 */
	@Override
	public void clear() {
		root = null;
		size = 0;
		modCount++;
	}

	/**
	 * Returns a new {@code TreeMap} with the same mappings, size and comparator
	 * as this instance.
	 *
	 * @return a shallow copy of this instance.
	 * @see java.lang.Cloneable
	 */
	@SuppressWarnings("unchecked")
	@Override
	public TreeMap<K, V> clone() {
		try {
			TreeMap<K, V> clone = (TreeMap<K, V>) super.clone();
			clone.entrySet = null;
			clone.descendingMap = null;
			clone.navigableKeySet = null;
			if (root != null) {
				clone.root = root.clone(null);
				// restore prev/next chain
				Node<K, V> node = minimum(clone.root);
				while (true) {
					Node<K, V> nxt = successor(node);
					if (nxt == null) {
						break;
					}
					nxt.prev = node;
					node.next = nxt;
					node = nxt;
				}
			}
			return clone;
		} catch (CloneNotSupportedException e) {
			return null;
		}
	}

	/**
	 * Returns the comparator used to compare elements in this map.
	 *
	 * @return the comparator or {@code null} if the natural ordering is used.
	 */
	@Override
	public Comparator<? super K> comparator() {
		return comparator;
	}

	/**
	 * Returns whether this map contains the specified key.
	 *
	 * @param key
	 *            the key to search for.
	 * @return {@code true} if this map contains the specified key,
	 *         {@code false} otherwise.
	 * @throws ClassCastException
	 *             if the specified key cannot be compared with the keys in this
	 *             map.
	 * @throws NullPointerException
	 *             if the specified key is {@code null} and the comparator
	 *             cannot handle {@code null} keys.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean containsKey(Object key) {
		Comparable<K> object = comparator == null ? toComparable((K) key) : null;
		K keyK = (K) key;
		Node<K, V> node = root;
		while (node != null) {
			K[] keys = node.keys;
			int left_idx = node.left_idx;
			int result = cmp(object, keyK, keys[left_idx]);
			if (result < 0) {
				node = node.left;
			} else if (result == 0) {
				return true;
			} else {
				int right_idx = node.right_idx;
				if (left_idx != right_idx) {
					result = cmp(object, keyK, keys[right_idx]);
				}
				if (result > 0) {
					node = node.right;
				} else if (result == 0) {
					return true;
				} else { /* search in node */
					int low = left_idx + 1, mid, high = right_idx - 1;
					while (low <= high) {
						mid = (low + high) >>> 1;
						result = cmp(object, keyK, keys[mid]);
						if (result > 0) {
							low = mid + 1;
						} else if (result == 0) {
							return true;
						} else {
							high = mid - 1;
						}
					}
					return false;
				}
			}
		}
		return false;
	}

	/**
	 * Returns whether this map contains the specified value.
	 *
	 * @param value
	 *            the value to search for.
	 * @return {@code true} if this map contains the specified value,
	 *         {@code false} otherwise.
	 */
	@Override
	public boolean containsValue(Object value) {
		if (root == null) {
			return false;
		}
		Node<K, V> node = minimum(root);
		if (value != null) {
			while (node != null) {
				int to = node.right_idx;
				V[] values = node.values;
				for (int i = node.left_idx; i <= to; i++) {
					if (value.equals(values[i])) {
						return true;
					}
				}
				node = node.next;
			}
		} else {
			while (node != null) {
				int to = node.right_idx;
				V[] values = node.values;
				for (int i = node.left_idx; i <= to; i++) {
					if (values[i] == null) {
						return true;
					}
				}
				node = node.next;
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	Entry<K, V> find(Object key) {
		Comparable<K> object = comparator == null ? toComparable((K) key) : null;
		K keyK = (K) key;
		Node<K, V> node = root;
		while (node != null) {
			K[] keys = node.keys;
			int left_idx = node.left_idx;
			int result = cmp(object, keyK, keys[left_idx]);
			if (result < 0) {
				node = node.left;
			} else if (result == 0) {
				return newEntry(node, left_idx);
			} else {
				int right_idx = node.right_idx;
				if (left_idx != right_idx) {
					result = cmp(object, keyK, keys[right_idx]);
				}
				if (result > 0) {
					node = node.right;
				} else if (result == 0) {
					return newEntry(node, right_idx);
				} else { /* search in node */
					int low = left_idx + 1, mid, high = right_idx - 1;
					while (low <= high) {
						mid = (low + high) >> 1;
						result = cmp(object, keyK, keys[mid]);
						if (result > 0) {
							low = mid + 1;
						} else if (result == 0) {
							return newEntry(node, mid);
						} else {
							high = mid - 1;
						}
					}
					return null;
				}
			}
		}
		return null;
	}

	static <K, V> TreeMap.Entry<K, V> newEntry(Node<K, V> node, int index) {
		return new TreeMap.Entry<K, V>(node, index);
	}

	static <K, V> Map.Entry<K, V> newImmutableEntry(TreeMap.Entry<K, V> entry) {
		return (null == entry) ? null : new SimpleImmutableEntry<K, V>(entry);
	}

	TreeMap.Entry<K, V> findSmallestEntry() {
		if (null != root) {
			Node<K, V> node = minimum(root);
			return newEntry(node, node.left_idx);
		}
		return null;
	}

	TreeMap.Entry<K, V> findBiggestEntry() {
		if (null != root) {
			Node<K, V> node = maximum(root);
			return newEntry(node, node.right_idx);
		}
		return null;
	}

	TreeMap.Entry<K, V> findCeilingEntry(K key) {
		if (root == null) {
			return null;
		}
		Comparable<K> object = comparator == null ? toComparable(key) : null;
		K keyK = key;
		Node<K, V> node = root;
		Node<K, V> foundNode = null;
		int foundIndex = 0;
		while (node != null) {
			K[] keys = node.keys;
			int left_idx = node.left_idx;
			int right_idx = node.right_idx;
			int result = cmp(object, keyK, keys[left_idx]);
			if (result < 0) {
				foundNode = node;
				foundIndex = left_idx;
				node = node.left;
			} else if (result == 0) {
				foundNode = node;
				foundIndex = left_idx;
				break;
			} else {
				if (left_idx != right_idx) {
					result = cmp(object, key, keys[right_idx]);
				}
				if (result > 0) {
					node = node.right;
				} else { /* search in node */
					foundNode = node;
					foundIndex = right_idx;
					if (result == 0) {
						break;
					}
					int low = left_idx + 1, mid, high = right_idx - 1;
					while (low <= high && result != 0) {
						mid = (low + high) >> 1;
						result = cmp(object, key, keys[mid]);
						if (result <= 0) {
							foundNode = node;
							foundIndex = mid;
							high = mid - 1;
						} else {
							low = mid + 1;
						}
						if (result == 0 || (low == high && high == mid)) {
							break;
						}
					}
					break;
				}
			}
		}
		if (foundNode != null && cmp(object, keyK, foundNode.keys[foundIndex]) > 0) {
			foundNode = null;
		}
		if (foundNode != null) {
			return newEntry(foundNode, foundIndex);
		}
		return null;
	}

	TreeMap.Entry<K, V> findFloorEntry(K key) {
		if (root == null) {
			return null;
		}
		Comparable<K> object = comparator == null ? toComparable(key) : null;
		K keyK = key;
		Node<K, V> node = root;
		Node<K, V> foundNode = null;
		int foundIndex = 0;
		while (node != null) {
			K[] keys = node.keys;
			int left_idx = node.left_idx;
			int result = cmp(object, keyK, keys[left_idx]);
			if (result < 0) {
				node = node.left;
			} else {
				foundNode = node;
				foundIndex = left_idx;
				if (result == 0) {
					break;
				}
				int right_idx = node.right_idx;
				if (left_idx != right_idx) {
					result = cmp(object, keyK, keys[right_idx]);
				}
				if (result >= 0) {
					foundNode = node;
					foundIndex = right_idx;
					if (result == 0) {
						break;
					}
					node = node.right;
				} else { /* search in node */
					int low = left_idx + 1, mid, high = right_idx - 1;
					while (low <= high && result != 0) {
						mid = (low + high) >> 1;
						result = cmp(object, keyK, keys[mid]);
						if (result >= 0) {
							foundNode = node;
							foundIndex = mid;
							low = mid + 1;
						} else {
							high = mid;
						}
						if (result == 0 || (low == high && high == mid)) {
							break;
						}
					}
					break;
				}
			}
		}
		if (foundNode != null && cmp(object, keyK, foundNode.keys[foundIndex]) < 0) {
			foundNode = null;
		}
		if (foundNode != null) {
			return newEntry(foundNode, foundIndex);
		}
		return null;
	}

	TreeMap.Entry<K, V> findLowerEntry(K key) {
		if (root == null) {
			return null;
		}
		Comparable<K> object = comparator == null ? toComparable(key) : null;
		K keyK = key;
		Node<K, V> node = root;
		Node<K, V> foundNode = null;
		int foundIndex = 0;
		while (node != null) {
			K[] keys = node.keys;
			int left_idx = node.left_idx;
			int result = cmp(object, keyK, keys[left_idx]);
			if (result <= 0) {
				node = node.left;
			} else {
				foundNode = node;
				foundIndex = left_idx;
				int right_idx = node.right_idx;
				if (left_idx != right_idx) {
					result = cmp(object, keyK, keys[right_idx]);
				}
				if (result > 0) {
					foundNode = node;
					foundIndex = right_idx;
					node = node.right;
				} else { /* search in node */
					int low = left_idx + 1, mid, high = right_idx - 1;
					while (low <= high) {
						mid = (low + high) >> 1;
						result = cmp(object, keyK, keys[mid]);
						if (result > 0) {
							foundNode = node;
							foundIndex = mid;
							low = mid + 1;
						} else {
							high = mid;
						}
						if (low == high && high == mid) {
							break;
						}
					}
					break;
				}
			}
		}
		if (foundNode != null && cmp(object, keyK, foundNode.keys[foundIndex]) <= 0) {
			foundNode = null;
		}
		if (foundNode != null) {
			return newEntry(foundNode, foundIndex);
		}
		return null;
	}

	TreeMap.Entry<K, V> findHigherEntry(K key) {
		if (root == null) {
			return null;
		}
		Comparable<K> object = comparator == null ? toComparable(key) : null;
		K keyK = key;
		Node<K, V> node = root;
		Node<K, V> foundNode = null;
		int foundIndex = 0;
		while (node != null) {
			K[] keys = node.keys;
			int right_idx = node.right_idx;
			int result = cmp(object, keyK, keys[right_idx]);
			if (result >= 0) {
				node = node.right;
			} else {
				foundNode = node;
				foundIndex = right_idx;
				int left_idx = node.left_idx;
				if (left_idx != right_idx) {
					result = cmp(object, key, keys[left_idx]);
				}
				if (result < 0) {
					foundNode = node;
					foundIndex = left_idx;
					node = node.left;
				} else { /* search in node */
					foundNode = node;
					foundIndex = right_idx;
					int low = left_idx + 1, mid, high = right_idx - 1;
					while (low <= high) {
						mid = (low + high) >> 1;
						result = cmp(object, key, keys[mid]);
						if (result < 0) {
							foundNode = node;
							foundIndex = mid;
							high = mid - 1;
						} else {
							low = mid + 1;
						}
						if (low == high && high == mid) {
							break;
						}
					}
					break;
				}
			}
		}
		if (foundNode != null && cmp(object, keyK, foundNode.keys[foundIndex]) >= 0) {
			foundNode = null;
		}
		if (foundNode != null) {
			return newEntry(foundNode, foundIndex);
		}
		return null;
	}

	/**
	 * Returns the first key in this map.
	 *
	 * @return the first key in this map.
	 * @throws NoSuchElementException
	 *             if this map is empty.
	 */
	@Override
	public K firstKey() {
		if (root != null) {
			Node<K, V> node = minimum(root);
			return node.keys[node.left_idx];
		}
		throw new NoSuchElementException();
	}

	/**
	 * Returns the value of the mapping with the specified key.
	 *
	 * @param key
	 *            the key.
	 * @return the value of the mapping with the specified key.
	 * @throws ClassCastException
	 *             if the key cannot be compared with the keys in this map.
	 * @throws NullPointerException
	 *             if the key is {@code null} and the comparator cannot handle
	 *             {@code null}.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		Comparable<K> object = comparator == null ? toComparable((K) key) : null;
		K keyK = (K) key;
		Node<K, V> node = root;
		while (node != null) {
			K[] keys = node.keys;
			int left_idx = node.left_idx;
			int result = cmp(object, keyK, keys[left_idx]);
			if (result < 0) {
				node = node.left;
			} else if (result == 0) {
				return node.values[left_idx];
			} else {
				int right_idx = node.right_idx;
				if (left_idx != right_idx) {
					result = cmp(object, keyK, keys[right_idx]);
				}
				if (result > 0) {
					node = node.right;
				} else if (result == 0) {
					return node.values[right_idx];
				} else { /* search in node */
					int low = left_idx + 1, mid, high = right_idx - 1;
					while (low <= high) {
						mid = (low + high) >>> 1;
						result = cmp(object, keyK, keys[mid]);
						if (result > 0) {
							low = mid + 1;
						} else if (result == 0) {
							return node.values[mid];
						} else {
							high = mid - 1;
						}
					}
					return null;
				}
			}
		}
		return null;
	}

	/**
	 * Returns a set of the keys contained in this map. The set is backed by
	 * this map so changes to one are reflected by the other. The set does not
	 * support adding.
	 *
	 * @return a set of the keys.
	 */
	@Override
	public Set<K> keySet() {
		return navigableKeySet();
	}

	/**
	 * Returns the last key in this map.
	 *
	 * @return the last key in this map.
	 * @throws NoSuchElementException
	 *             if this map is empty.
	 */
	@Override
	public K lastKey() {
		if (root != null) {
			Node<K, V> node = maximum(root);
			return node.keys[node.right_idx];
		}
		throw new NoSuchElementException();
	}

	static <K, V> Node<K, V> successor(Node<K, V> x) {
		if (x.right != null) {
			return minimum(x.right);
		}
		Node<K, V> y = x.parent;
		while (y != null && x == y.right) {
			x = y;
			y = y.parent;
		}
		return y;
	}

	private int cmp(Comparable<K> object, K key1, K key2) {
		return object != null ? object.compareTo(key2) : comparator.compare(key1, key2);
	}

	/**
	 * Maps the specified key to the specified value.
	 *
	 * @param key
	 *            the key.
	 * @param value
	 *            the value.
	 * @return the value of any previous mapping with the specified key or
	 *         {@code null} if there was no mapping.
	 * @throws ClassCastException
	 *             if the specified key cannot be compared with the keys in this
	 *             map.
	 * @throws NullPointerException
	 *             if the specified key is {@code null} and the comparator
	 *             cannot handle {@code null} keys.
	 */
	@Override
	public V put(K key, V value) {
		if (key == null && comparator == null) {
			throw new NullPointerException();
		}
		if (root == null) {
			root = createNode(key, value);
			size = 1;
			modCount++;
			return null;
		}
		Comparable<K> object = comparator == null ? toComparable(key) : null;
		K keyK = key;
		Node<K, V> node = root;
		Node<K, V> prevNode = null;
		int result = 0;
		while (node != null) {
			prevNode = node;
			K[] keys = node.keys;
			int left_idx = node.left_idx;
			result = cmp(object, keyK, keys[left_idx]);
			if (result < 0) {
				node = node.left;
			} else if (result == 0) {
				V res = node.values[left_idx];
				node.values[left_idx] = value;
				return res;
			} else {
				int right_idx = node.right_idx;
				if (left_idx != right_idx) {
					result = cmp(object, keyK, keys[right_idx]);
				}
				if (result > 0) {
					node = node.right;
				} else if (result == 0) {
					V res = node.values[right_idx];
					node.values[right_idx] = value;
					return res;
				} else { /* search in node */
					int low = left_idx + 1, mid, high = right_idx - 1;
					while (low <= high) {
						mid = (low + high) >>> 1;
						result = cmp(object, keyK, keys[mid]);
						if (result > 0) {
							low = mid + 1;
						} else if (result == 0) {
							V res = node.values[mid];
							node.values[mid] = value;
							return res;
						} else {
							high = mid - 1;
						}
					}
					result = low;
					break;
				}
			}
		} /* while */
		/*
		 * if(node == null) { if(prevNode==null) { - case of empty Tree } else {
		 * result < 0 - prevNode.left==null - attach here result > 0 -
		 * prevNode.right==null - attach here } } else { insert into node.
		 * result - index where it should be inserted. }
		 */
		size++;
		modCount++;
		if (node == null) {
			if (prevNode == null) {
				// case of empty Tree
				root = createNode(key, value);
			} else if (prevNode.size < Node.NODE_SIZE) {
				// there is a place for insert
				if (result < 0) {
					appendFromLeft(prevNode, key, value);
				} else {
					appendFromRight(prevNode, key, value);
				}
			} else {
				// create and link
				Node<K, V> newNode = createNode(key, value);
				if (result < 0) {
					attachToLeft(prevNode, newNode);
				} else {
					attachToRight(prevNode, newNode);
				}
				balance(newNode);
			}
		} else {
			// insert into node.
			// result - index where it should be inserted.
			if (node.size < Node.NODE_SIZE) { // insert and ok
				int left_idx = node.left_idx;
				int right_idx = node.right_idx;
				if (left_idx == 0 || ((right_idx != Node.NODE_SIZE - 1) && (right_idx - result <= result - left_idx))) {
					int right_idxPlus1 = right_idx + 1;
					System.arraycopy(node.keys, result, node.keys, result + 1, right_idxPlus1 - result);
					System.arraycopy(node.values, result, node.values, result + 1, right_idxPlus1 - result);
					node.right_idx = right_idxPlus1;
					node.keys[result] = key;
					node.values[result] = value;
				} else {
					int left_idxMinus1 = left_idx - 1;
					System.arraycopy(node.keys, left_idx, node.keys, left_idxMinus1, result - left_idx);
					System.arraycopy(node.values, left_idx, node.values, left_idxMinus1, result - left_idx);
					node.left_idx = left_idxMinus1;
					node.keys[result - 1] = key;
					node.values[result - 1] = value;
				}
				node.size++;
			} else {
				// there are no place here
				// insert and push old pair
				Node<K, V> previous = node.prev;
				Node<K, V> nextNode = node.next;
				boolean removeFromStart;
				boolean attachFromLeft = false;
				Node<K, V> attachHere = null;
				if (previous == null) {
					if (nextNode != null && nextNode.size < Node.NODE_SIZE) {
						// move last pair to next
						removeFromStart = false;
					} else {
						// next node doesn't exist or full
						// left==null
						// drop first pair to new node from left
						removeFromStart = true;
						attachFromLeft = true;
						attachHere = node;
					}
				} else if (nextNode == null) {
					if (previous.size < Node.NODE_SIZE) {
						// move first pair to prev
						removeFromStart = true;
					} else {
						// right == null;
						// drop last pair to new node from right
						removeFromStart = false;
						attachFromLeft = false;
						attachHere = node;
					}
				} else {
					if (previous.size < Node.NODE_SIZE) {
						if (nextNode.size < Node.NODE_SIZE) {
							// choose prev or next for moving
							removeFromStart = previous.size < nextNode.size;
						} else {
							// move first pair to prev
							removeFromStart = true;
						}
					} else {
						if (nextNode.size < Node.NODE_SIZE) {
							// move last pair to next
							removeFromStart = false;
						} else {
							// prev & next are full
							// if node.right!=null then node.next.left==null
							// if node.left!=null then node.prev.right==null
							if (node.right == null) {
								attachHere = node;
								attachFromLeft = false;
								removeFromStart = false;
							} else {
								attachHere = nextNode;
								attachFromLeft = true;
								removeFromStart = false;
							}
						}
					}
				}
				K movedKey;
				V movedValue;
				if (removeFromStart) {
					// node.left_idx == 0
					movedKey = node.keys[0];
					movedValue = node.values[0];
					int resMinus1 = result - 1;
					System.arraycopy(node.keys, 1, node.keys, 0, resMinus1);
					System.arraycopy(node.values, 1, node.values, 0, resMinus1);
					node.keys[resMinus1] = key;
					node.values[resMinus1] = value;
				} else {
					// node.right_idx == Node.NODE_SIZE - 1
					movedKey = node.keys[Node.NODE_SIZE - 1];
					movedValue = node.values[Node.NODE_SIZE - 1];
					System.arraycopy(node.keys, result, node.keys, result + 1, Node.NODE_SIZE - 1 - result);
					System.arraycopy(node.values, result, node.values, result + 1, Node.NODE_SIZE - 1 - result);
					node.keys[result] = key;
					node.values[result] = value;
				}
				if (attachHere == null) {
					if (removeFromStart) {
						appendFromRight(previous, movedKey, movedValue);
					} else {
						appendFromLeft(nextNode, movedKey, movedValue);
					}
				} else {
					Node<K, V> newNode = createNode(movedKey, movedValue);
					if (attachFromLeft) {
						attachToLeft(attachHere, newNode);
					} else {
						attachToRight(attachHere, newNode);
					}
					balance(newNode);
				}
			}
		}
		return null;
	}

	private void appendFromLeft(Node<K, V> node, K keyObj, V value) {
		if (node.left_idx == 0) {
			int new_right = node.right_idx + 1;
			System.arraycopy(node.keys, 0, node.keys, 1, new_right);
			System.arraycopy(node.values, 0, node.values, 1, new_right);
			node.right_idx = new_right;
		} else {
			node.left_idx--;
		}
		node.size++;
		node.keys[node.left_idx] = keyObj;
		node.values[node.left_idx] = value;
	}

	private void attachToLeft(Node<K, V> node, Node<K, V> newNode) {
		newNode.parent = node;
		// node.left==null - attach here
		node.left = newNode;
		Node<K, V> predecessor = node.prev;
		newNode.prev = predecessor;
		newNode.next = node;
		if (predecessor != null) {
			predecessor.next = newNode;
		}
		node.prev = newNode;
	}

	/*
	 * add pair into node; existence free room in the node should be checked
	 * before call
	 */
	private void appendFromRight(Node<K, V> node, K keyObj, V value) {
		if (node.right_idx == Node.NODE_SIZE - 1) {
			int left_idx = node.left_idx;
			int new_left = left_idx - 1;
			System.arraycopy(node.keys, left_idx, node.keys, new_left, Node.NODE_SIZE - left_idx);
			System.arraycopy(node.values, left_idx, node.values, new_left, Node.NODE_SIZE - left_idx);
			node.left_idx = new_left;
		} else {
			node.right_idx++;
		}
		node.size++;
		node.keys[node.right_idx] = keyObj;
		node.values[node.right_idx] = value;
	}

	private void attachToRight(Node<K, V> node, Node<K, V> newNode) {
		newNode.parent = node;
		// - node.right==null - attach here
		node.right = newNode;
		newNode.prev = node;
		Node<K, V> successor = node.next;
		newNode.next = successor;
		if (successor != null) {
			successor.prev = newNode;
		}
		node.next = newNode;
	}

	private Node<K, V> createNode(K keyObj, V value) {
		Node<K, V> node = new Node<K, V>();
		node.keys[0] = keyObj;
		node.values[0] = value;
		node.left_idx = 0;
		node.right_idx = 0;
		node.size = 1;
		return node;
	}

	void balance(Node<K, V> x) {
		Node<K, V> y;
		x.color = true;
		while (x != root && x.parent.color) {
			if (x.parent == x.parent.parent.left) {
				y = x.parent.parent.right;
				if (y != null && y.color) {
					x.parent.color = false;
					y.color = false;
					x.parent.parent.color = true;
					x = x.parent.parent;
				} else {
					if (x == x.parent.right) {
						x = x.parent;
						leftRotate(x);
					}
					x.parent.color = false;
					x.parent.parent.color = true;
					rightRotate(x.parent.parent);
				}
			} else {
				y = x.parent.parent.left;
				if (y != null && y.color) {
					x.parent.color = false;
					y.color = false;
					x.parent.parent.color = true;
					x = x.parent.parent;
				} else {
					if (x == x.parent.left) {
						x = x.parent;
						rightRotate(x);
					}
					x.parent.color = false;
					x.parent.parent.color = true;
					leftRotate(x.parent.parent);
				}
			}
		}
		root.color = false;
	}

	private void rightRotate(Node<K, V> x) {
		Node<K, V> y = x.left;
		x.left = y.right;
		if (y.right != null) {
			y.right.parent = x;
		}
		y.parent = x.parent;
		if (x.parent == null) {
			root = y;
		} else {
			if (x == x.parent.right) {
				x.parent.right = y;
			} else {
				x.parent.left = y;
			}
		}
		y.right = x;
		x.parent = y;
	}

	private void leftRotate(Node<K, V> x) {
		Node<K, V> y = x.right;
		x.right = y.left;
		if (y.left != null) {
			y.left.parent = x;
		}
		y.parent = x.parent;
		if (x.parent == null) {
			root = y;
		} else {
			if (x == x.parent.left) {
				x.parent.left = y;
			} else {
				x.parent.right = y;
			}
		}
		y.left = x;
		x.parent = y;
	}

	/**
	 * Copies all the mappings in the given map to this map. These mappings will
	 * replace all mappings that this map had for any of the keys currently in
	 * the given map.
	 *
	 * @param map
	 *            the map to copy mappings from.
	 * @throws ClassCastException
	 *             if a key in the specified map cannot be compared with the
	 *             keys in this map.
	 * @throws NullPointerException
	 *             if a key in the specified map is {@code null} and the
	 *             comparator cannot handle {@code null} keys.
	 */
	@Override
	public void putAll(Map<? extends K, ? extends V> map) {
		for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}

	/**
	 * Removes the mapping with the specified key from this map.
	 *
	 * @param key
	 *            the key of the mapping to remove.
	 * @return the value of the removed mapping or {@code null} if no mapping
	 *         for the specified key was found.
	 * @throws ClassCastException
	 *             if the specified key cannot be compared with the keys in this
	 *             map.
	 * @throws NullPointerException
	 *             if the specified key is {@code null} and the comparator
	 *             cannot handle {@code null} keys.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public V remove(Object key) {
		Comparable<K> object = comparator == null ? toComparable((K) key) : null;
		if (size == 0) {
			return null;
		}
		K keyK = (K) key;
		Node<K, V> node = root;
		while (node != null) {
			K[] keys = node.keys;
			int left_idx = node.left_idx;
			int result = cmp(object, keyK, keys[left_idx]);
			if (result < 0) {
				node = node.left;
			} else if (result == 0) {
				V value = node.values[left_idx];
				removeLeftmost(node, false);
				return value;
			} else {
				int right_idx = node.right_idx;
				if (left_idx != right_idx) {
					result = cmp(object, keyK, keys[right_idx]);
				}
				if (result > 0) {
					node = node.right;
				} else if (result == 0) {
					V value = node.values[right_idx];
					removeRightmost(node, false);
					return value;
				} else { /* search in node */
					int low = left_idx + 1, mid, high = right_idx - 1;
					while (low <= high) {
						mid = (low + high) >>> 1;
						result = cmp(object, keyK, keys[mid]);
						if (result > 0) {
							low = mid + 1;
						} else if (result == 0) {
							V value = node.values[mid];
							removeMiddleElement(node, mid, false);
							return value;
						} else {
							high = mid - 1;
						}
					}
					return null;
				}
			}
		}
		return null;
	}

	private static <K, V> int natural(Node<K, V> node, boolean reverse) {
		if (node == null) {
			return 0;
		}
		return reverse ? node.right_idx : node.left_idx;
	}

	Entry<K, V> removeLeftmost(Node<K, V> node, boolean reverse) {
		int index = node.left_idx;
		Node<K, V> sn;
		int si;
		if (node.size == 1) {
			deleteNode(node);
			sn = reverse ? node.prev : node.next;
			si = natural(sn, reverse);
		} else if (node.prev != null && (Node.NODE_SIZE - 1 - node.prev.right_idx) > node.size) {
			// move all to prev node and kill it
			Node<K, V> prev = node.prev;
			int size = node.right_idx - index;
			System.arraycopy(node.keys, index + 1, prev.keys, prev.right_idx + 1, size);
			System.arraycopy(node.values, index + 1, prev.values, prev.right_idx + 1, size);
			prev.right_idx += size;
			prev.size += size;
			deleteNode(node);
			sn = prev;
			si = reverse ? prev.right_idx - size : prev.right_idx - size + 1;
		} else if (node.next != null && (node.next.left_idx) > node.size) {
			// move all to next node and kill it
			Node<K, V> next = node.next;
			int size = node.right_idx - index;
			int next_new_left = next.left_idx - size;
			next.left_idx = next_new_left;
			System.arraycopy(node.keys, index + 1, next.keys, next_new_left, size);
			System.arraycopy(node.values, index + 1, next.values, next_new_left, size);
			next.size += size;
			deleteNode(node);
			sn = reverse ? node.prev : next;
			si = natural(sn, reverse);
		} else {
			node.keys[index] = null;
			node.values[index] = null;
			node.left_idx++;
			node.size--;
			Node<K, V> prev = node.prev;
			sn = reverse ? prev : node;
			si = natural(sn, reverse);
			if (prev != null && prev.size == 1) {
				node.size++;
				node.left_idx--;
				node.keys[node.left_idx] = prev.keys[prev.left_idx];
				node.values[node.left_idx] = prev.values[prev.left_idx];
				deleteNode(prev);
				if (reverse) {
					sn = node;
					si = node.left_idx;
				}
			}
		}
		modCount++;
		size--;
		return sn == null ? null : newEntry(sn, si);
	}

	Entry<K, V> removeRightmost(Node<K, V> node, boolean reverse) {
		int index = node.right_idx;
		Node<K, V> sn;
		int si;
		if (node.size == 1) {
			deleteNode(node);
			sn = reverse ? node.prev : node.next;
			si = natural(sn, reverse);
		} else if (node.prev != null && (Node.NODE_SIZE - 1 - node.prev.right_idx) > node.size) {
			// move all to prev node and kill it
			Node<K, V> prev = node.prev;
			int left_idx = node.left_idx;
			int size = index - left_idx;
			System.arraycopy(node.keys, left_idx, prev.keys, prev.right_idx + 1, size);
			System.arraycopy(node.values, left_idx, prev.values, prev.right_idx + 1, size);
			prev.right_idx += size;
			prev.size += size;
			deleteNode(node);
			sn = reverse ? prev : node.next;
			si = natural(sn, reverse);
		} else if (node.next != null && (node.next.left_idx) > node.size) {
			// move all to next node and kill it
			Node<K, V> next = node.next;
			int left_idx = node.left_idx;
			int size = index - left_idx;
			int next_new_left = next.left_idx - size;
			next.left_idx = next_new_left;
			System.arraycopy(node.keys, left_idx, next.keys, next_new_left, size);
			System.arraycopy(node.values, left_idx, next.values, next_new_left, size);
			next.size += size;
			deleteNode(node);
			sn = next;
			si = reverse ? next.left_idx + size - 1 : next.left_idx + size;
		} else {
			node.keys[index] = null;
			node.values[index] = null;
			node.right_idx--;
			node.size--;
			Node<K, V> next = node.next;
			sn = reverse ? node : next;
			si = natural(sn, reverse);
			if (next != null && next.size == 1) {
				node.size++;
				node.right_idx++;
				node.keys[node.right_idx] = next.keys[next.left_idx];
				node.values[node.right_idx] = next.values[next.left_idx];
				deleteNode(next);
				if (!reverse) {
					sn = node;
					si = node.right_idx;
				}
			}
		}
		modCount++;
		size--;
		return sn == null ? null : newEntry(sn, si);
	}

	Entry<K, V> removeMiddleElement(Node<K, V> node, int index, boolean reverse) {
		// this function is called iff index if some middle element;
		// so node.left_idx < index < node.right_idx
		// condition above assume that node.size > 1
		Node<K, V> sn;
		int si;
		if (node.prev != null && (Node.NODE_SIZE - 1 - node.prev.right_idx) > node.size) {
			// move all to prev node and kill it
			Node<K, V> prev = node.prev;
			int left_idx = node.left_idx;
			int size = index - left_idx;
			System.arraycopy(node.keys, left_idx, prev.keys, prev.right_idx + 1, size);
			System.arraycopy(node.values, left_idx, prev.values, prev.right_idx + 1, size);
			prev.right_idx += size;
			size = node.right_idx - index;
			System.arraycopy(node.keys, index + 1, prev.keys, prev.right_idx + 1, size);
			System.arraycopy(node.values, index + 1, prev.values, prev.right_idx + 1, size);
			prev.right_idx += size;
			prev.size += (node.size - 1);
			deleteNode(node);
			sn = prev;
			si = reverse ? prev.right_idx - size : prev.right_idx - size + 1;
		} else if (node.next != null && (node.next.left_idx) > node.size) {
			// move all to next node and kill it
			Node<K, V> next = node.next;
			int left_idx = node.left_idx;
			int next_new_left = next.left_idx - node.size + 1;
			next.left_idx = next_new_left;
			int size = index - left_idx;
			System.arraycopy(node.keys, left_idx, next.keys, next_new_left, size);
			System.arraycopy(node.values, left_idx, next.values, next_new_left, size);
			next_new_left += size;
			size = node.right_idx - index;
			System.arraycopy(node.keys, index + 1, next.keys, next_new_left, size);
			System.arraycopy(node.values, index + 1, next.values, next_new_left, size);
			next.size += (node.size - 1);
			deleteNode(node);
			sn = next;
			si = reverse ? next.left_idx - node.left_idx + index - 1 : next.left_idx - node.left_idx + index;
		} else {
			int moveFromRight = node.right_idx - index;
			int left_idx = node.left_idx;
			int moveFromLeft = index - left_idx;
			if (moveFromRight <= moveFromLeft) {
				System.arraycopy(node.keys, index + 1, node.keys, index, moveFromRight);
				System.arraycopy(node.values, index + 1, node.values, index, moveFromRight);
				Node<K, V> next = node.next;
				if (next != null && next.size == 1) {
					node.keys[node.right_idx] = next.keys[next.left_idx];
					node.values[node.right_idx] = next.values[next.left_idx];
					deleteNode(next);
				} else {
					node.keys[node.right_idx] = null;
					node.values[node.right_idx] = null;
					node.right_idx--;
					node.size--;
				}
				sn = node;
				si = reverse ? index - 1 : index;
			} else {
				System.arraycopy(node.keys, left_idx, node.keys, left_idx + 1, moveFromLeft);
				System.arraycopy(node.values, left_idx, node.values, left_idx + 1, moveFromLeft);
				Node<K, V> prev = node.prev;
				if (prev != null && prev.size == 1) {
					node.keys[left_idx] = prev.keys[prev.left_idx];
					node.values[left_idx] = prev.values[prev.left_idx];
					deleteNode(prev);
				} else {
					node.keys[left_idx] = null;
					node.values[left_idx] = null;
					node.left_idx++;
					node.size--;
				}
				sn = node;
				si = reverse ? index : index + 1;
			}
		}
		modCount++;
		size--;
		return newEntry(sn, si);
	}

	private void deleteNode(Node<K, V> node) {
		if (node.right == null) {
			if (node.left != null) {
				attachToParent(node, node.left);
			} else {
				attachNullToParent(node);
			}
			fixNextChain(node);
		} else if (node.left == null) { // node.right != null
			attachToParent(node, node.right);
			fixNextChain(node);
		} else {
			// Here node.left!=nul && node.right!=null
			// node.next should replace node in tree
			// node.next!=null by tree logic.
			// node.next.left==null by tree logic.
			// node.next.right may be null or non-null
			Node<K, V> toMoveUp = node.next;
			fixNextChain(node);
			if (toMoveUp.right == null) {
				attachNullToParent(toMoveUp);
			} else {
				attachToParent(toMoveUp, toMoveUp.right);
			}
			// Here toMoveUp is ready to replace node
			toMoveUp.left = node.left;
			if (node.left != null) {
				node.left.parent = toMoveUp;
			}
			toMoveUp.right = node.right;
			if (node.right != null) {
				node.right.parent = toMoveUp;
			}
			attachToParentNoFixup(node, toMoveUp);
			toMoveUp.color = node.color;
		}
	}

	private void attachToParentNoFixup(Node<K, V> toDelete, Node<K, V> toConnect) {
		// assert toConnect!=null
		Node<K, V> parent = toDelete.parent;
		toConnect.parent = parent;
		if (parent == null) {
			root = toConnect;
		} else if (toDelete == parent.left) {
			parent.left = toConnect;
		} else {
			parent.right = toConnect;
		}
	}

	private void attachToParent(Node<K, V> toDelete, Node<K, V> toConnect) {
		// assert toConnect!=null
		attachToParentNoFixup(toDelete, toConnect);
		if (!toDelete.color) {
			fixup(toConnect);
		}
	}

	private void attachNullToParent(Node<K, V> toDelete) {
		Node<K, V> parent = toDelete.parent;
		if (parent == null) {
			root = null;
		} else {
			if (toDelete == parent.left) {
				parent.left = null;
			} else {
				parent.right = null;
			}
			if (!toDelete.color) {
				fixup(parent);
			}
		}
	}

	private void fixNextChain(Node<K, V> node) {
		if (node.prev != null) {
			node.prev.next = node.next;
		}
		if (node.next != null) {
			node.next.prev = node.prev;
		}
	}

	private void fixup(Node<K, V> x) {
		Node<K, V> w;
		while (x != root && !x.color) {
			if (x == x.parent.left) {
				w = x.parent.right;
				if (w == null) {
					x = x.parent;
					continue;
				}
				if (w.color) {
					w.color = false;
					x.parent.color = true;
					leftRotate(x.parent);
					w = x.parent.right;
					if (w == null) {
						x = x.parent;
						continue;
					}
				}
				if ((w.left == null || !w.left.color) && (w.right == null || !w.right.color)) {
					w.color = true;
					x = x.parent;
				} else {
					if (w.right == null || !w.right.color) {
						w.left.color = false;
						w.color = true;
						rightRotate(w);
						w = x.parent.right;
					}
					w.color = x.parent.color;
					x.parent.color = false;
					w.right.color = false;
					leftRotate(x.parent);
					x = root;
				}
			} else {
				w = x.parent.left;
				if (w == null) {
					x = x.parent;
					continue;
				}
				if (w.color) {
					w.color = false;
					x.parent.color = true;
					rightRotate(x.parent);
					w = x.parent.left;
					if (w == null) {
						x = x.parent;
						continue;
					}
				}
				if ((w.left == null || !w.left.color) && (w.right == null || !w.right.color)) {
					w.color = true;
					x = x.parent;
				} else {
					if (w.left == null || !w.left.color) {
						w.right.color = false;
						w.color = true;
						leftRotate(w);
						w = x.parent.left;
					}
					w.color = x.parent.color;
					x.parent.color = false;
					w.left.color = false;
					rightRotate(x.parent);
					x = root;
				}
			}
		}
		x.color = false;
	}

	/**
	 * Returns the number of mappings in this map.
	 *
	 * @return the number of mappings in this map.
	 */
	@Override
	public int size() {
		return size;
	}

	/**
	 * Returns a collection of the values contained in this map. The collection
	 * is backed by this map so changes to one are reflected by the other. The
	 * collection supports remove, removeAll, retainAll and clear operations,
	 * and it does not support add or addAll operations.
	 * <p>
	 * This method returns a collection which is the subclass of
	 * AbstractCollection. The iterator method of this subclass returns a
	 * "wrapper object" over the iterator of map's entrySet(). The {@code size}
	 * method wraps the map's size method and the {@code contains} method wraps
	 * the map's containsValue method.
	 * <p>
	 * The collection is created when this method is called for the first time
	 * and returned in response to all subsequent calls. This method may return
	 * different collections when multiple concurrent calls occur, since no
	 * synchronization is performed.
	 *
	 * @return a collection of the values contained in this map.
	 */
	@Override
	public Collection<V> values() {
		if (valuesCollection == null) {
			valuesCollection = new AbstractCollection<V>() {
				@Override
				public boolean contains(Object object) {
					return containsValue(object);
				}

				@Override
				public int size() {
					return size;
				}

				@Override
				public void clear() {
					TreeMap.this.clear();
				}

				@Override
				public Iterator<V> iterator() {
					return new UnboundedValueIterator<K, V>(TreeMap.this);
				}
			};
		}
		return valuesCollection;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see java.util.NavigableMap#firstEntry()
	 * @since 1.6
	 */
	@Override
	public Map.Entry<K, V> firstEntry() {
		return newImmutableEntry(findSmallestEntry());
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see java.util.NavigableMap#lastEntry()
	 * @since 1.6
	 */
	@Override
	public Map.Entry<K, V> lastEntry() {
		return newImmutableEntry(findBiggestEntry());
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see java.util.NavigableMap#pollFirstEntry()
	 * @since 1.6
	 */
	@Override
	public Map.Entry<K, V> pollFirstEntry() {
		Entry<K, V> node = findSmallestEntry();
		Map.Entry<K, V> result = newImmutableEntry(node);
		if (null != node) {
			remove(node.key);
		}
		return result;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see java.util.NavigableMap#pollLastEntry()
	 * @since 1.6
	 */
	@Override
	public Map.Entry<K, V> pollLastEntry() {
		Entry<K, V> node = findBiggestEntry();
		Map.Entry<K, V> result = newImmutableEntry(node);
		if (null != node) {
			remove(node.key);
		}
		return result;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see java.util.NavigableMap#higherEntry(Object)
	 * @since 1.6
	 */
	@Override
	public Map.Entry<K, V> higherEntry(K key) {
		return newImmutableEntry(findHigherEntry(key));
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see java.util.NavigableMap#higherKey(Object)
	 * @since 1.6
	 */
	@Override
	public K higherKey(K key) {
		Map.Entry<K, V> entry = higherEntry(key);
		return (null == entry) ? null : entry.getKey();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see java.util.NavigableMap#lowerEntry(Object)
	 * @since 1.6
	 */
	@Override
	public Map.Entry<K, V> lowerEntry(K key) {
		return newImmutableEntry(findLowerEntry(key));
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see java.util.NavigableMap#lowerKey(Object)
	 * @since 1.6
	 */
	@Override
	public K lowerKey(K key) {
		Map.Entry<K, V> entry = lowerEntry(key);
		return (null == entry) ? null : entry.getKey();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see java.util.NavigableMap#ceilingEntry(java.lang.Object)
	 * @since 1.6
	 */
	@Override
	public Map.Entry<K, V> ceilingEntry(K key) {
		return newImmutableEntry(findCeilingEntry(key));
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see java.util.NavigableMap#ceilingKey(java.lang.Object)
	 * @since 1.6
	 */
	@Override
	public K ceilingKey(K key) {
		Map.Entry<K, V> entry = ceilingEntry(key);
		return (null == entry) ? null : entry.getKey();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see java.util.NavigableMap#floorEntry(Object)
	 * @since 1.6
	 */
	@Override
	public Map.Entry<K, V> floorEntry(K key) {
		return newImmutableEntry(findFloorEntry(key));
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see java.util.NavigableMap#floorKey(Object)
	 * @since 1.6
	 */
	@Override
	public K floorKey(K key) {
		Map.Entry<K, V> entry = floorEntry(key);
		return (null == entry) ? null : entry.getKey();
	}

	@SuppressWarnings("unchecked")
	private static <T> Comparable<T> toComparable(T obj) {
		if (obj == null) {
			throw new NullPointerException();
		}
		return (Comparable<T>) obj;
	}

	int keyCompare(K left, K right) {
		return (null != comparator()) ? comparator().compare(left, right) : toComparable(left).compareTo(right);
	}

	static <K, V> Node<K, V> minimum(Node<K, V> x) {
		if (x == null) {
			return null;
		}
		while (x.left != null) {
			x = x.left;
		}
		return x;
	}

	static <K, V> Node<K, V> maximum(Node<K, V> x) {
		if (x == null) {
			return null;
		}
		while (x.right != null) {
			x = x.right;
		}
		return x;
	}

	/**
	 * Returns a set containing all of the mappings in this map. Each mapping is
	 * an instance of {@link Map.Entry}. As the set is backed by this map,
	 * changes in one will be reflected in the other. It does not support adding
	 * operations.
	 *
	 * @return a set of the mappings.
	 */
	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		if (entrySet == null) {
			entrySet = new AbstractSet<Map.Entry<K, V>>() {
				@Override
				public int size() {
					return size;
				}

				@Override
				public void clear() {
					TreeMap.this.clear();
				}

				@SuppressWarnings("unchecked")
				@Override
				public boolean contains(Object object) {
					if (object instanceof Map.Entry) {
						Map.Entry<K, V> entry = (Map.Entry<K, V>) object;
						K key = entry.getKey();
						Object v1 = get(key), v2 = entry.getValue();
						return v1 == null ? (v2 == null && TreeMap.this.containsKey(key)) : v1.equals(v2);
					}
					return false;
				}

				@Override
				public Iterator<Map.Entry<K, V>> iterator() {
					return new UnboundedEntryIterator<K, V>(TreeMap.this);
				}
			};
		}
		return entrySet;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see java.util.NavigableMap#navigableKeySet()
	 * @since 1.6
	 */
	@Override
	public NavigableSet<K> navigableKeySet() {
		return (null != navigableKeySet) ? navigableKeySet : (navigableKeySet = (new AscendingSubMap<K, V>(this)).navigableKeySet());
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see java.util.NavigableMap#descendingKeySet()
	 * @since 1.6
	 */
	@Override
	public NavigableSet<K> descendingKeySet() {
		return descendingMap().navigableKeySet();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see java.util.NavigableMap#descendingMap()
	 * @since 1.6
	 */
	@Override
	public NavigableMap<K, V> descendingMap() {
		return (null != descendingMap) ? descendingMap : (descendingMap = new DescendingSubMap<K, V>(this));
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see java.util.NavigableMap#subMap(Object, boolean, Object, boolean)
	 * @since 1.6
	 */
	@Override
	public NavigableMap<K, V> subMap(K start, boolean startInclusive, K end, boolean endInclusive) {
		if (keyCompare(start, end) <= 0) {
			return new AscendingSubMap<K, V>(start, startInclusive, this, end, endInclusive);
		}
		throw new IllegalArgumentException();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see java.util.NavigableMap#headMap(Object, boolean)
	 * @since 1.6
	 */
	@Override
	public NavigableMap<K, V> headMap(K end, boolean inclusive) {
		// check for error
		keyCompare(end, end);
		return new AscendingSubMap<K, V>(this, end, inclusive);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see java.util.NavigableMap#tailMap(Object, boolean)
	 * @since 1.6
	 */
	@Override
	public NavigableMap<K, V> tailMap(K start, boolean inclusive) {
		// check for error
		keyCompare(start, start);
		return new AscendingSubMap<K, V>(start, inclusive, this);
	}

	/**
	 * Returns a sorted map over a range of this sorted map with all keys
	 * greater than or equal to the specified {@code startKey} and less than the
	 * specified {@code endKey}. Changes to the returned sorted map are
	 * reflected in this sorted map and vice versa.
	 * <p>
	 * Note: The returned map will not allow an insertion of a key outside the
	 * specified range.
	 *
	 * @param startKey
	 *            the low boundary of the range (inclusive).
	 * @param endKey
	 *            the high boundary of the range (exclusive),
	 * @return a sorted map with the key from the specified range.
	 * @throws ClassCastException
	 *             if the start or end key cannot be compared with the keys in
	 *             this map.
	 * @throws NullPointerException
	 *             if the start or end key is {@code null} and the comparator
	 *             cannot handle {@code null} keys.
	 * @throws IllegalArgumentException
	 *             if the start key is greater than the end key, or if this map
	 *             is itself a sorted map over a range of another sorted map and
	 *             the specified range is outside of its range.
	 */
	@Override
	public SortedMap<K, V> subMap(K startKey, K endKey) {
		return subMap(startKey, true, endKey, false);
	}

	/**
	 * Returns a sorted map over a range of this sorted map with all keys that
	 * are less than the specified {@code endKey}. Changes to the returned
	 * sorted map are reflected in this sorted map and vice versa.
	 * <p>
	 * Note: The returned map will not allow an insertion of a key outside the
	 * specified range.
	 *
	 * @param endKey
	 *            the high boundary of the range specified.
	 * @return a sorted map where the keys are less than {@code endKey}.
	 * @throws ClassCastException
	 *             if the specified key cannot be compared with the keys in this
	 *             map.
	 * @throws NullPointerException
	 *             if the specified key is {@code null} and the comparator
	 *             cannot handle {@code null} keys.
	 * @throws IllegalArgumentException
	 *             if this map is itself a sorted map over a range of another
	 *             map and the specified key is outside of its range.
	 */
	@Override
	public SortedMap<K, V> headMap(K endKey) {
		return headMap(endKey, false);
	}

	/**
	 * Returns a sorted map over a range of this sorted map with all keys that
	 * are greater than or equal to the specified {@code startKey}. Changes to
	 * the returned sorted map are reflected in this sorted map and vice versa.
	 * <p>
	 * Note: The returned map will not allow an insertion of a key outside the
	 * specified range.
	 *
	 * @param startKey
	 *            the low boundary of the range specified.
	 * @return a sorted map where the keys are greater or equal to
	 *         {@code startKey}.
	 * @throws ClassCastException
	 *             if the specified key cannot be compared with the keys in this
	 *             map.
	 * @throws NullPointerException
	 *             if the specified key is {@code null} and the comparator
	 *             cannot handle {@code null} keys.
	 * @throws IllegalArgumentException
	 *             if this map itself a sorted map over a range of another map
	 *             and the specified key is outside of its range.
	 */
	@Override
	public SortedMap<K, V> tailMap(K startKey) {
		return tailMap(startKey, true);
	}

	private void writeObject(ObjectOutputStream stream) throws IOException {
		stream.defaultWriteObject();
		stream.writeInt(size);
		if (size > 0) {
			Node<K, V> node = minimum(root);
			while (node != null) {
				int to = node.right_idx;
				for (int i = node.left_idx; i <= to; i++) {
					stream.writeObject(node.keys[i]);
					stream.writeObject(node.values[i]);
				}
				node = node.next;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
		stream.defaultReadObject();
		int size = stream.readInt();
		Node<K, V> lastNode = null;
		for (int i = 0; i < size; i++) {
			lastNode = addToLast(lastNode, (K) stream.readObject(), (V) stream.readObject());
		}
	}

	static class AbstractMapIterator<K, V> {
		TreeMap<K, V> backingMap;

		int expectedModCount;

		Node<K, V> node;

		Node<K, V> lastNode;

		int offset;

		int lastOffset;

		AbstractMapIterator(TreeMap<K, V> map) {
			backingMap = map;
			expectedModCount = map.modCount;
			node = minimum(map.root);
			if (node != null) {
				offset = node.left_idx;
			}
		}

		public boolean hasNext() {
			return node != null;
		}

		final void makeNext() {
			if (expectedModCount != backingMap.modCount) {
				throw new ConcurrentModificationException();
			} else if (node == null) {
				throw new NoSuchElementException();
			}
			lastNode = node;
			lastOffset = offset;
			if (offset != node.right_idx) {
				offset++;
			} else {
				node = node.next;
				if (node != null) {
					offset = node.left_idx;
				}
			}
		}

		final public void remove() {
			if (lastNode == null) {
				throw new IllegalStateException();
			}
			if (expectedModCount != backingMap.modCount) {
				throw new ConcurrentModificationException();
			}

			Entry<K, V> entry;
			int idx = lastOffset;
			if (idx == lastNode.left_idx) {
				entry = backingMap.removeLeftmost(lastNode, false);
			} else if (idx == lastNode.right_idx) {
				entry = backingMap.removeRightmost(lastNode, false);
			} else {
				entry = backingMap.removeMiddleElement(node, idx, false);
			}
			if (entry != null) {
				node = entry.node;
				offset = entry.index;
			}
			lastNode = null;
			expectedModCount++;
		}
	}

	static class UnboundedEntryIterator<K, V> extends AbstractMapIterator<K, V> implements Iterator<Map.Entry<K, V>> {

		UnboundedEntryIterator(TreeMap<K, V> map) {
			super(map);
		}

		@Override
		public Map.Entry<K, V> next() {
			makeNext();
			return newEntry(lastNode, lastOffset);
		}
	}

	static class UnboundedKeyIterator<K, V> extends AbstractMapIterator<K, V> implements Iterator<K> {

		UnboundedKeyIterator(TreeMap<K, V> map) {
			super(map);
		}

		@Override
		public K next() {
			makeNext();
			return lastNode.keys[lastOffset];
		}
	}

	static class UnboundedValueIterator<K, V> extends AbstractMapIterator<K, V> implements Iterator<V> {

		UnboundedValueIterator(TreeMap<K, V> map) {
			super(map);
		}

		@Override
		public V next() {
			makeNext();
			return lastNode.values[lastOffset];
		}
	}
}
