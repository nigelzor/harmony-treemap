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

package com.pulseenergy.org.apache.harmony.java.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;

public class RefSortedMap<K, V> extends java.util.AbstractMap<K, V>
        implements SortedMap<K, V>, Cloneable, Serializable {

    private static final long serialVersionUID = 1L;

    private static final class MapEntry<K, V> implements Map.Entry<K, V> {

        final K key;
        V value;

        MapEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
		public K getKey() {
            return key;
        }

        @Override
		public V getValue() {
            return value;
        }

        @Override
		public V setValue(V v) {
            V res = value;
            value = v;
            return res;
        }

        @Override
		public int hashCode() {
            return (getKey() == null ? 0 : getKey().hashCode())
                    ^ (getValue() == null ? 0 : getValue().hashCode());
        }

        @Override
		public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object instanceof Map.Entry) {
                Map.Entry<?, ?> entry = (Map.Entry<?, ?>) object;
                return (getKey() == null ? entry.getKey() == null : getKey().equals(entry
                        .getKey()))
                        && (getValue() == null ? entry.getValue() == null : getValue()
                                .equals(entry.getValue()));
            }
            return false;
        }
    }

    transient ArrayList<MapEntry<K, V>> entries = new ArrayList<MapEntry<K, V>>();
    transient int modCnt;

    private final Comparator<? super K> comparator;

    class SubMap extends java.util.AbstractMap<K, V>
            implements SortedMap<K, V>, Cloneable {

        final boolean hasStart, hasEnd;
        final K start, end;

        SubMap(boolean hasFirst, K first, boolean hasLast, K last) {
            this.hasStart = hasFirst;
            this.start = first;
            this.hasEnd = hasLast;
            this.end = last;
            if (hasStart && hasEnd && compare(start, end) >= 0) {
                throw new IllegalArgumentException();
            }
        }

        @Override
        public Set<java.util.Map.Entry<K, V>> entrySet() {
            return new AbstractSet<Entry<K,V>> () {

                @Override
                public Iterator<java.util.Map.Entry<K, V>> iterator() {
                    return new Iterator<Entry<K,V>>() {
                        int modCnt = RefSortedMap.this.modCnt;
                        int offset = SubMap.this.size() > 0 ?
                                bsearch(SubMap.this.firstKey()) - 1 :
                                entries.size();

                        @Override
						public boolean hasNext() {
                            if (modCnt != RefSortedMap.this.modCnt) {
                                throw new ConcurrentModificationException();
                            }
                            return offset + 1 < entries.size()
                                && isInRange(entries.get(offset + 1).getKey());
                        }

                        @Override
						public Map.Entry<K, V> next() {
                            if (modCnt != RefSortedMap.this.modCnt) {
                                throw new ConcurrentModificationException();
                            }
                            if (!hasNext()) {
                                throw new NoSuchElementException();
                            }
                            offset++;
                            return entries.get(offset);
                        }

                        @Override
						public void remove() {
                            if (modCnt != RefSortedMap.this.modCnt) {
                                throw new ConcurrentModificationException();
                            }
                            modCnt++;
                            RefSortedMap.this.modCnt++;
                            RefSortedMap.this.entries.remove(offset);
                            offset--;
                        }

                    };
                }

                @Override
                public int size() {
                    try {
                        int lastIdx = bsearch(SubMap.this.lastKey());
                        int firstIdx = bsearch(SubMap.this.firstKey());
                        return lastIdx - firstIdx + 1;
                    } catch (NoSuchElementException e) {
                        return 0;
                    } catch (ArrayIndexOutOfBoundsException e) {
                        return 0;
                    }
                }

            };
        }

        @Override
		public Comparator<? super K> comparator() {
            return RefSortedMap.this.comparator();
        }

        @Override
		public K firstKey() {
            if (!hasStart) {
                K res = RefSortedMap.this.firstKey();
                if (!isInRange(res)) {
                    throw new NoSuchElementException();
                }
                return res;
            }
            int idx = bsearch(start);
            if (idx >= 0) {
                return start;
            }
            if (-idx - 1 >= entries.size() || !isInRange(entries.get(-idx - 1).getKey())) {
                throw new NoSuchElementException();
            }
            return entries.get(-idx - 1).getKey();
        }

        @Override
		public SortedMap<K, V> headMap(K key) {
            if (!isInRange(key)) {
                throw new IllegalArgumentException();
            }
            return new SubMap(hasStart, start, true, key);
        }

        @Override
		public K lastKey() {
            if (!hasEnd) {
                K res = RefSortedMap.this.lastKey();
                if (!isInRange(res)) {
                    throw new NoSuchElementException();
                }
                return res;
            }
            int idx = bsearch(end);
            idx = idx >= 0 ? idx - 1 : -idx -2;
            if (idx < 0 || !isInRange(entries.get(idx).getKey())) {
                throw new NoSuchElementException();
            }
            return entries.get(idx).getKey();
        }

        @Override
		public SortedMap<K, V> subMap(K startKey, K endKey) {
            if (!isInRange(startKey)) {
                throw new IllegalArgumentException();
            }
            if (!isInRange(endKey)) {
                throw new IllegalArgumentException();
            }
            return new SubMap(true, startKey, true, endKey);
        }

        @Override
		public SortedMap<K, V> tailMap(K key) {
            if (!isInRange(key)) {
                throw new IllegalArgumentException();
            }
            return new SubMap(true, key, hasEnd, end);
        }

        private boolean isInRange(K key) {
            if (hasStart && compare(key, start) < 0) {
                    return false;
            }
            if (hasEnd && compare(key, end) >= 0) {
                    return false;
            }
            return true;
        }

    }

    public RefSortedMap() {
        this((Comparator<? super K>) null);
    }

    @SuppressWarnings("unchecked")
    public int compare(K start, K end) {
        return comparator != null ? comparator.compare(start, end)
                : ((Comparable<K>) start).compareTo(end);
    }

    @SuppressWarnings("unchecked")
    public RefSortedMap(Comparator<? super K> comparator) {
        this.comparator = comparator;
        cmp = createCmp();
    }

    public RefSortedMap(Map<? extends K, ? extends V> map) {
        this();
        putAll(map);
    }

    public RefSortedMap(SortedMap<K, ? extends V> map) {
        this(map.comparator());
        putAll(map);
    }

    @Override
	public Comparator<? super K> comparator() {
        return comparator;
    }

    @Override
	public Set<Map.Entry<K, V>> entrySet() {
        return tailMap(firstKey()).entrySet();
    }

    @Override
	public K firstKey() {
        return entries.get(0).getKey();
    }

    @Override
	public SortedMap<K, V> headMap(K key) {
        return new SubMap(false, null, true, key);
    }

    @Override
	public Set<K> keySet() {
        return tailMap(firstKey()).keySet();
    }

    @Override
	public K lastKey() {
        return entries.get(entries.size() - 1).getKey();
    }

    @Override
	public SortedMap<K, V> subMap(K startKey, K endKey) {
        return new SubMap(true, startKey, true, endKey);
    }

    @Override
	public SortedMap<K, V> tailMap(K key) {
        return new SubMap(true, key, false, null);
    }

    @Override
	public Collection<V> values() {
        return tailMap(firstKey()).values();
    }

    @Override
	public void clear() {
        entries.clear();
    }

    @Override
	public boolean containsKey(Object arg0) {
        return bsearch(arg0) >= 0;
    }

    @Override
	public boolean containsValue(Object arg0) {
        for (V v : values()) {
            if (arg0.equals(v)) {
                return true;
            }
        }
        return false;
    }

    @Override
	@SuppressWarnings("unchecked")
    public V get(Object arg0) {
        int idx = bsearch(arg0);
        return idx >= 0 ? entries.get(idx).getValue() : null;
    }

    @Override
	public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Override
	public V put(K arg0, V arg1) {
        modCnt++;
        int idx = bsearch(arg0);
        if (idx >= 0) {
            return entries.get(idx).setValue(arg1);
        }
        entries.add(-idx -1, new MapEntry<K, V>(arg0, arg1));
        return null;
    }

    @Override
	public void putAll(Map<? extends K, ? extends V> arg0) {
        for (Map.Entry<? extends K, ? extends V> e : arg0.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
	@SuppressWarnings("unchecked")
    public V remove(Object arg0) {
        modCnt++;
        int idx = bsearch(arg0);
        if (idx < 0) {
            return null;
        }
        return entries.remove(idx).getValue();
    }

    transient private Comparator<MapEntry<K, V>> cmp = createCmp();

    Comparator<MapEntry<K, V>> createCmp() {
        return new Comparator<MapEntry<K, V>>() {

            @Override
			public int compare(MapEntry<K, V> arg0, MapEntry<K, V> arg1) {
                return RefSortedMap.this.compare(arg0.getKey(), arg1.getKey());
            }

        };
    }

    @SuppressWarnings("unchecked")
    private int bsearch(Object arg0) {
        return Collections.binarySearch(entries, new MapEntry<K, V>((K) arg0, null), cmp);
    }

    @Override
	public int size() {
        return entries.size();
    }

    @Override
	public RefSortedMap<K, V> clone() {
        return new RefSortedMap<K, V>(this);
    }

    private void writeObject(ObjectOutputStream stream) throws IOException {
        stream.defaultWriteObject();
        stream.writeInt(size());
        for (Map.Entry<K, V> e : entrySet()) {
            stream.writeObject(e.getKey());
            stream.writeObject(e.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream stream) throws IOException,
            ClassNotFoundException {

        cmp = createCmp();
        stream.defaultReadObject();
        int size = stream.readInt();
        entries = new ArrayList<MapEntry<K, V>>(size);
        for (int i = 0; i < size; i++) {
            put((K) stream.readObject(), (V) stream.readObject());
        }
    }

}
