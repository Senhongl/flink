package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.KeyedStreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ExceptionInChainedOperatorException;

import javax.annotation.Nullable;

public class ReaderOutputAdaptor<T> implements ReaderOutput<T> {
    private final TimestampAssigner<T> timestampAssigner;
    private final PushingAsyncDataInput.DataOutput<T> recordsOutput;
    @Nullable private final KeyGroupAssigner keyGroupAssigner;
    private SourceOutput<T> currentOutputWithWatermarks;
    @Nullable private final ReaderOutput<T> mainOutputWithWatermarks;
    private final StreamRecord<T> reusingRecord;

    public ReaderOutputAdaptor(
            WatermarkStrategy<T> watermarkStrategy,
            MetricGroup metrics,
            PushingAsyncDataInput.DataOutput<T> recordsOutput,
            @Nullable KeyGroupAssigner keyGroupAssigner,
            @Nullable ReaderOutput<T> mainOutputWithWatermarks) {

        final TimestampsAndWatermarksContext context = new TimestampsAndWatermarksContext(metrics);
        this.timestampAssigner =
                watermarkStrategy.createTimestampAssigner(context);

        this.recordsOutput = recordsOutput;
        this.keyGroupAssigner = keyGroupAssigner;
        this.mainOutputWithWatermarks = mainOutputWithWatermarks;
        if (keyGroupAssigner != null) {
            this.reusingRecord = new KeyedStreamRecord<>(null);
        } else {
            this.reusingRecord = new StreamRecord<>(null);
        }
    }


    @Override
    public void collect(T record) {
        collect(record, TimestampAssigner.NO_TIMESTAMP);
    }

    @Override
    public void collect(T record, long timestamp) {
        try {
            final long assignedTimestamp = timestampAssigner.extractTimestamp(record, timestamp);
            reusingRecord.replace(record, assignedTimestamp);
            if (keyGroupAssigner != null) {
                ((KeyedStreamRecord<T>) reusingRecord).setKeyGroup(keyGroupAssigner.getCurrentKeyGroup());
            }
            recordsOutput.emitRecord(reusingRecord);
            if (currentOutputWithWatermarks != null) {
                currentOutputWithWatermarks.collect(record, assignedTimestamp);
            }
        } catch (ExceptionInChainedOperatorException e) {
            throw e;
        } catch (Exception e) {
            throw new ExceptionInChainedOperatorException(e);
        }
    }

    @Override
    public void emitWatermark(Watermark watermark) {
        if (currentOutputWithWatermarks != null) {
            currentOutputWithWatermarks.emitWatermark(watermark);
        }
    }

    @Override
    public void markIdle() {
        if (currentOutputWithWatermarks != null) {
            currentOutputWithWatermarks.markIdle();
        }
    }

    @Override
    public SourceOutput<T> createOutputForSplit(String splitId) {
        if (mainOutputWithWatermarks != null) {
            currentOutputWithWatermarks = mainOutputWithWatermarks.createOutputForSplit(splitId);
        }
        if (keyGroupAssigner != null) {
            keyGroupAssigner.assignKeyGroup(splitId);
        }
        return this;
    }

    @Override
    public void releaseOutputForSplit(String splitId) {
        if (mainOutputWithWatermarks != null) {
            mainOutputWithWatermarks.releaseOutputForSplit(splitId);
        }
        if (keyGroupAssigner != null) {
            keyGroupAssigner.releaseKeyGroup(splitId);
        }
    }
}
