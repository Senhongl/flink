package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.api.common.eventtime.NoWatermarksGenerator;
import org.apache.flink.api.common.eventtime.RecordTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ReaderOutputAdapterTest {

    @Test
    public void inlineTest() {
        final CollectingDataOutput<Integer> dataOutput = new CollectingDataOutput<>();

        final ReaderOutputAdapter<Integer> output = new ReaderOutputAdapter<>(
                new RecordTimestampAssigner<>(),
                dataOutput,
                null,
                null);
        for (int i = 0; i < 20000; i++) {
            inlineTestHelper(output);
        }
    }

    public void inlineTestHelper(SourceOutput<Integer> output) {
        output.collect(17);
    }

    private static final class TestWatermarkGenerator<T> implements WatermarkGenerator<T> {

        private long lastTimestamp;

        @Override
        public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
            lastTimestamp = eventTimestamp;
            output.emitWatermark(new Watermark(eventTimestamp));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(lastTimestamp));
        }
    }
}
