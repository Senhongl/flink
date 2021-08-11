/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * For a pre-KeyedStream, this class will maintain a mapping from a split to a key group. The
 * mapping is built when a split is assigned and deleted when a split is removed. When {@link
 * ReaderOutput#createOutputForSplit(String)} is called, the assigner would switch the current key
 * group to the one the split maps to.
 */
public class KeyGroupAssigner {

    private int currentKeyGroup;

    private final Set<Integer> availableKeyGroups;

    private final Map<String, Integer> splitToKeyGroup;

    public KeyGroupAssigner(int startKeyGroup, int endKeyGroup) {
        this.availableKeyGroups = new HashSet<>();
        this.splitToKeyGroup = new HashMap<>();
        for (int i = startKeyGroup; i < endKeyGroup; i++) {
            availableKeyGroups.add(i);
        }
    }

    public void initializeState(Map<String, Integer> splitToKeyGroup) throws Exception {
        splitToKeyGroup.forEach(this.splitToKeyGroup::put);
    }

    public List<Tuple2<String, Integer>> snapshotState() {
        List<Tuple2<String, Integer>> state = new ArrayList<>();
        splitToKeyGroup.forEach((key, value) -> state.add(new Tuple2<>(key, value)));
        return state;
    }

    /**
     * This method would build a new mapping for the split. If the mapping exists, this method would
     * do nothing but switch the current key group to the one the split maps to.
     */
    public void assignKeyGroup(String splitId) {
        if (splitToKeyGroup.containsKey(splitId)) {
            currentKeyGroup = splitToKeyGroup.get(splitId);
            return;
        }

        if (availableKeyGroups.size() > 0) {
            currentKeyGroup = availableKeyGroups.iterator().next();
            splitToKeyGroup.put(splitId, currentKeyGroup);
        } else {
            throw new RuntimeException(
                    "No more keyGroup for this source operator. If the maximum number"
                            + "of splits exceeds the maximum number of KeyGroups, please configure the maximum"
                            + "number of KeyGroup, a.k.a, the maximum number of parallel subtasks.");
        }
    }

    /**
     * When a split is finished or removed, this method will be called and delete the corresponding
     * mapping for the removed splits so that the idle key group could be assigned to the new added
     * splits.
     */
    public void releaseKeyGroup(String splitId) {
        int availableKeyGroup = splitToKeyGroup.get(splitId);
        availableKeyGroups.add(availableKeyGroup);
        splitToKeyGroup.remove(splitId, availableKeyGroup);
    }

    public int getCurrentKeyGroup() {
        return currentKeyGroup;
    }

    public Map<String, Integer> getSplitToKeyGroup() {
        return splitToKeyGroup;
    }
}
