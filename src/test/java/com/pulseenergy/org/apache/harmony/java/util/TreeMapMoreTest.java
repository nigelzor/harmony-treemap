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

import java.util.ArrayList;
import java.util.SortedMap;

public class TreeMapMoreTest extends junit.framework.TestCase {

    public void test_SubMap() {
        TreeMap<Integer, Integer> tm = buildTree(65);

        SortedMap<Integer,Integer> subMap = tm.subMap(0, 64);

        assertEquals((Integer) 0, subMap.firstKey());
        assertEquals((Integer) 63, subMap.lastKey());
        assertEquals(64, subMap.size());
    }

    public void test_SubMap2() {
        TreeMap<Integer, Integer> tm = buildTree(65);

        SortedMap<Integer,Integer> subMap = tm.descendingMap().subMap(64, 0);

        assertEquals((Integer) 64, subMap.firstKey());
        assertEquals((Integer) 1, subMap.lastKey());
        assertEquals(64, subMap.size());
    }

    public void test_SubMapValues() {
        TreeMap<Integer, Integer> tm = buildTree(80);

        ArrayList<Integer> values = new ArrayList<Integer>(tm.subMap(20, 60).values());

        assertEquals(40, values.size());
        assertEquals((Integer) 20, values.get(0));
        assertEquals((Integer) 59, values.get(values.size() - 1));
    }

    public void test_SubMapValues2() {
        TreeMap<Integer, Integer> tm = buildTree(80);

        ArrayList<Integer> values = new ArrayList<Integer>(tm.descendingMap().subMap(60, 20).values());

        assertEquals(40, values.size());
        assertEquals((Integer) 60, values.get(0));
        assertEquals((Integer) 21, values.get(values.size() - 1));
    }

    private static TreeMap<Integer, Integer> buildTree(int size) {
        TreeMap<Integer,Integer> tm = new TreeMap<Integer,Integer>();
        for (int i = 0; i < size; i++) {
            tm.put(i, i);
        }
        return tm;
    }

}
