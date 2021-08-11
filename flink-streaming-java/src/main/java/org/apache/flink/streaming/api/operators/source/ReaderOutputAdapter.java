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

import org.apache.flink.annotation.VisibleForTesting;
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

/**
 * The ReaderOutputAdapter would (1) extract timestamp from the record, (2) assign key group for the
 * record if necessary, (3) emit the watermarks if necessary.
 */
public class ReaderOutputAdapter<T> implements ReaderOutput<T> {
    private final TimestampAssigner<T> timestampAssigner;
    private final PushingAsyncDataInput.DataOutput<T> recordsOutput;
    @Nullable private final KeyGroupAssigner keyGroupAssigner;
    private SourceOutput<T> currentOutputWithWatermarks;
    @Nullable private final ReaderOutput<T> mainOutputWithWatermarks;
    private final StreamRecord<T> reusingRecord;

    public ReaderOutputAdapter(
            WatermarkStrategy<T> watermarkStrategy,
            MetricGroup metrics,
            PushingAsyncDataInput.DataOutput<T> recordsOutput,
            @Nullable KeyGroupAssigner keyGroupAssigner,
            @Nullable ReaderOutput<T> mainOutputWithWatermarks) {

        final TimestampsAndWatermarksContext context = new TimestampsAndWatermarksContext(metrics);
        this.timestampAssigner = watermarkStrategy.createTimestampAssigner(context);

        this.recordsOutput = recordsOutput;
        this.keyGroupAssigner = keyGroupAssigner;
        this.mainOutputWithWatermarks = mainOutputWithWatermarks;
        if (keyGroupAssigner != null) {
            this.reusingRecord = new KeyedStreamRecord<>(null);
        } else {
            this.reusingRecord = new StreamRecord<>(null);
        }
    }

    @VisibleForTesting
    public ReaderOutputAdapter(
            TimestampAssigner<T> timestampAssigner,
            PushingAsyncDataInput.DataOutput<T> recordsOutput,
            @Nullable KeyGroupAssigner keyGroupAssigner,
            @Nullable ReaderOutput<T> mainOutputWithWatermarks) {
        this.timestampAssigner = timestampAssigner;
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
    public final void collect(T record) {
        collect(record, TimestampAssigner.NO_TIMESTAMP);
    }

    @Override
    public final void collect(T record, long timestamp) {
        try {
            final long assignedTimestamp = timestampAssigner.extractTimestamp(record, timestamp);
            reusingRecord.replace(record, assignedTimestamp);
            if (keyGroupAssigner != null) {
                ((KeyedStreamRecord<T>) reusingRecord)
                        .setKeyGroup(keyGroupAssigner.getCurrentKeyGroup());
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
