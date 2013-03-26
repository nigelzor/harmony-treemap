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

import java.util.SortedMap;

public class TreeMapMoreTest extends junit.framework.TestCase {

    public void test_SubMap() {
        TreeMap<Integer,Integer> tm = new TreeMap<Integer,Integer>();
        for (int i = 0; i < 65; i++) {
            tm.put(i, i);
        }

        SortedMap<Integer,Integer> subMap = tm.subMap(0, 64);

        assertEquals((Integer) 63, subMap.lastKey());
        assertEquals(64, subMap.size());
    }

}
