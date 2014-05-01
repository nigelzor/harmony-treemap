package org.apache.harmony.java.util;

import com.google.common.collect.testing.NavigableMapTestSuiteBuilder;
import com.google.common.collect.testing.TestStringSortedMapGenerator;
import com.google.common.collect.testing.features.CollectionFeature;
import com.google.common.collect.testing.features.CollectionSize;
import com.google.common.collect.testing.features.MapFeature;
import com.google.common.collect.testing.testers.MapEntrySetTester;
import junit.framework.Test;
import junit.framework.TestSuite;

import java.util.Map;
import java.util.SortedMap;

public class GuavaTreeMapTest extends TestSuite {

	private static class TreeMapGenerator extends TestStringSortedMapGenerator {
		@Override
		protected SortedMap<String, String> create(Map.Entry<String, String>[] entries) {
			TreeMap<String, String> map = new TreeMap<String, String>();
			for (Map.Entry<String, String> entry : entries) {
				map.put(entry.getKey(), entry.getValue());
			}
			return map;
		}
	}

	public static Test suite() {
		return NavigableMapTestSuiteBuilder.using(new TreeMapGenerator())
				.named("TreeMap")
				.withFeatures(CollectionSize.ANY, MapFeature.GENERAL_PURPOSE, MapFeature.ALLOWS_NULL_VALUES,
						CollectionFeature.SUPPORTS_ITERATOR_REMOVE, CollectionFeature.SERIALIZABLE)
				.suppressing(MapEntrySetTester.getContainsEntryWithIncomparableKeyMethod())
				.createTestSuite();
	}
}
