package org.apache.flink.connector.base.source.reader.splitreader;

import java.util.List;

public class SplitsRemoval<SplitT> extends SplitsChange<SplitT> {
    public SplitsRemoval (List<SplitT> splits) {
        super(splits);
    }

    @Override
    public String toString() {
        return String.format("SplitRemoval:[%s]", splits());
    }
}
