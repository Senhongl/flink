package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    public void assignKeyGroup(String splitId) {
        if (splitToKeyGroup.containsKey(splitId)) {
            currentKeyGroup = splitToKeyGroup.get(splitId);
            return;
        }

        if (availableKeyGroups.size() > 0) {
            currentKeyGroup = availableKeyGroups.iterator().next();
            splitToKeyGroup.put(splitId, currentKeyGroup);
        } else {
            throw new RuntimeException("No more keyGroup for this source operator");
        }
    }

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
