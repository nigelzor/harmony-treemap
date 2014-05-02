package org.apache.harmony.java.util;

import com.google.common.collect.Ordering;
import com.google.common.collect.testing.MapTestSuiteBuilder;
import com.google.common.collect.testing.NavigableMapTestSuiteBuilder;
import com.google.common.collect.testing.TestStringSortedMapGenerator;
import com.google.common.collect.testing.features.CollectionFeature;
import com.google.common.collect.testing.features.CollectionSize;
import com.google.common.collect.testing.features.MapFeature;
import com.google.common.collect.testing.testers.MapEntrySetTester;
import junit.framework.Test;
import junit.framework.TestSuite;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;

public class GuavaTreeMapTest extends TestSuite {

	private static class TreeMapGenerator extends TestStringSortedMapGenerator {
		private final Comparator<? super String> comparator;

		public TreeMapGenerator() {
			this(null);
		}

		public TreeMapGenerator(Comparator<? super String> comparator) {
			this.comparator = comparator;
		}

		@Override
		protected SortedMap<String, String> create(Map.Entry<String, String>[] entries) {
			TreeMap<String, String> map = new TreeMap<String, String>(comparator);
			for (Map.Entry<String, String> entry : entries) {
				map.put(entry.getKey(), entry.getValue());
			}
			return map;
		}
	}

	public static Test suite() {
		TestSuite suite = new TestSuite();
		suite.setName("TreeMap");
		suite.addTest(naturalOrder());
		suite.addTest(comparator());
		return suite;
	}

	public static Test naturalOrder() {
		return NavigableMapTestSuiteBuilder.using(new TreeMapGenerator())
				.named("Natural Order")
				.withFeatures(CollectionSize.ANY, MapFeature.GENERAL_PURPOSE, MapFeature.ALLOWS_NULL_VALUES,
						CollectionFeature.SUPPORTS_ITERATOR_REMOVE, CollectionFeature.SERIALIZABLE)
				.suppressing(MapEntrySetTester.getContainsEntryWithIncomparableKeyMethod())
				.createTestSuite();
	}

	public static Test comparator() {
		// seems like null + sorted generates some invalid test cases, so only testing as an unsorted map
		return MapTestSuiteBuilder.using(new TreeMapGenerator(Ordering.natural().nullsFirst()))
				.named("Comparator")
				.withFeatures(CollectionSize.ANY, MapFeature.GENERAL_PURPOSE, MapFeature.ALLOWS_NULL_KEYS, MapFeature.ALLOWS_NULL_VALUES,
						CollectionFeature.SUPPORTS_ITERATOR_REMOVE, CollectionFeature.SERIALIZABLE)
				.suppressing(MapEntrySetTester.getContainsEntryWithIncomparableKeyMethod())
				.createTestSuite();
	}
}
