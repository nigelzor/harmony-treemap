package org.apache.harmony.java.util;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class RandomisedTest {
	private static final int TRIALS = 1<<10;
	private static final int SIZE = 8 * TreeMap.Node.NODE_SIZE;

	private NavigableMap<Integer, Integer> reference;
	private NavigableMap<Integer, Integer> testing;

	private static enum From {
		EntrySet {
			@Override
			<K, V> Iterator<?> iterator(NavigableMap<K, V> map) {
				return map.entrySet().iterator();
			}
		},
		KeySet {
			@Override
			<K, V> Iterator<?> iterator(NavigableMap<K, V> map) {
				return map.keySet().iterator();
			}
		},
		DescendingKeySet {
			@Override
			<K, V> Iterator<?> iterator(NavigableMap<K, V> map) {
				return map.descendingKeySet().iterator();
			}
		},
		Values {
			@Override
			<K, V> Iterator<?> iterator(NavigableMap<K, V> map) {
				return map.values().iterator();
			}
		},
		ReversedEntrySet {
			@Override
			<K, V> Iterator<?> iterator(NavigableMap<K, V> map) {
				return map.descendingMap().entrySet().iterator();
			}
		},
		SubMapEntrySet {
			@Override
			<K, V> Iterator<?> iterator(NavigableMap<K, V> map) {
				return map.subMap(map.firstKey(), false, map.lastKey(), false).entrySet().iterator();
			}
		};

		abstract <K, V> Iterator<?> iterator(NavigableMap<K, V> map);
	}

	@Test
	public void testRemovals() {
		Random random = new Random(81240);
		for (int i = 1; i <= TRIALS; i++) {
			System.out.print('.');
			if (i % 80 == 0 || i == TRIALS) System.out.println();

			create();
			fill(random);
			verify();
			while (!reference.isEmpty()) {
				remove(random);
				verify();
			}
		}
	}

	private void remove(Random random) {
		From from = From.values()[random.nextInt(From.values().length)];
		Iterator<?> referenceIterator = from.iterator(reference);
		Iterator<?> testingIterator = from.iterator(testing);

		List<?> initial = Lists.newArrayList(from.iterator(reference));
		List<String> operations = Lists.newArrayList();
		try {
			while (referenceIterator.hasNext()) {
				operations.add("next");
				Object expected = referenceIterator.next();
				assertEquals(expected, testingIterator.next());
				if (random.nextFloat() < 0.25) {
					operations.add("remove");
					referenceIterator.remove();
					testingIterator.remove();
				}
				verify();
			}
			assertFalse(testingIterator.hasNext());
		} catch (AssertionError e) {
			System.err.println();
			System.err.println("removing randomly " + from);
			System.err.println("initial: " + initial);
			System.err.println("operations: " + operations);
			throw e;
		}
	}

	private void verify() {
		assertEquals(reference.size(), testing.size());
		assertEquals(Lists.newArrayList(reference.entrySet()), Lists.newArrayList(testing.entrySet()));
	}

	private void fill(Random random) {
		for (int i = 0; i < SIZE; i++) {
			int x = random.nextInt();
			assertEquals(reference.put(x, x), testing.put(x, x));
		}
	}

	private void create() {
		reference = new java.util.TreeMap<Integer, Integer>();
		testing = new org.apache.harmony.java.util.TreeMap<Integer, Integer>();
	}

}
