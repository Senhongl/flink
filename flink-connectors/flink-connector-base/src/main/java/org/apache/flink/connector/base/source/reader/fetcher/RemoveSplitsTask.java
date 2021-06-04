package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RemoveSplitsTask<SplitT extends SourceSplit> implements SplitFetcherTask {

    private final SplitReader<?, SplitT> splitReader;
    private final List<SplitT> splitsToRemove;
    private final Map<String, SplitT> assignedSplits;

    RemoveSplitsTask(
            SplitReader<?, SplitT> splitReader,
            List<SplitT> splitsToRemove,
            Map<String, SplitT> assignedSplits) {
        this.splitReader = splitReader;
        this.splitsToRemove = splitsToRemove;
        this.assignedSplits = assignedSplits;
    }


    @Override
    public boolean run() {
        /**
         * TODO: What if the splits now are finished reading?
         *
         */

        for (SplitT s : splitsToRemove) {
            assignedSplits.remove(s.splitId(), s);
        }
        splitReader.handleSplitsChanges(new SplitsRemoval<>(splitsToRemove));
        return true;
    }

    @Override
    public void wakeUp() {
        // Do nothing.
    }

    @Override
    public String toString() {
        return String.format("RemoveSplitsTask: [%s]", splitsToRemove);
    }
}
