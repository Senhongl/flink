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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.util.Preconditions;

/**
 * Implementation of the SourceOutput. The records emitted to this output are pushed into a given
 * {@link PushingAsyncDataInput.DataOutput}. The watermarks are pushed into the same output, or into
 * a separate {@link WatermarkOutput}, if one is provided.
 *
 * <h2>Periodic Watermarks</h2>
 *
 * <p>This output does not implement automatic periodic watermark emission. The method {@link
 * SourceOutputWithWatermarks#emitPeriodicWatermark()} needs to be called periodically.
 *
 * <h2>Note on Performance Considerations</h2>
 *
 * <p>The methods {@link SourceOutput#collect(Object)} and {@link SourceOutput#collect(Object,
 * long)} are highly performance-critical (part of the hot loop). To make the code as JIT friendly
 * as possible, we want to have only a single implementation of these two methods, across all
 * classes. That way, the JIT compiler can de-virtualize (and inline) them better.
 *
 * <p>Currently, we have one implementation of these methods for the case where we don't need
 * watermarks (see class {@link NoOpTimestampsAndWatermarks}) and one for the case where we do (this
 * class). When the JVM is dedicated to a single job (or type of job) only one of these classes will
 * be loaded. In mixed job setups, we still have a bimorphic method (rather than a
 * poly/-/mega-morphic method).
 *
 * @param <T> The type of emitted records.
 */
@Internal
public class SourceOutputWithWatermarks<T> implements SourceOutput<T> {

    private final WatermarkGenerator<T> watermarkGenerator;

    private final WatermarkOutput onEventWatermarkOutput;

    private final WatermarkOutput periodicWatermarkOutput;

    /**
     * Creates a new SourceOutputWithWatermarks that emits records to the given DataOutput and
     * watermarks to the (possibly different) WatermarkOutput.
     */
    public SourceOutputWithWatermarks(
            WatermarkGenerator<T> watermarkGenerator,
            WatermarkOutput onEventWatermarkOutput,
            WatermarkOutput periodicWatermarkOutput) {
        this.watermarkGenerator = Preconditions.checkNotNull(watermarkGenerator);
        this.onEventWatermarkOutput = Preconditions.checkNotNull(onEventWatermarkOutput);
        this.periodicWatermarkOutput = Preconditions.checkNotNull(periodicWatermarkOutput);
    }

    // ------------------------------------------------------------------------
    // SourceOutput Methods
    //
    // Note that the two methods below are final, as a partial enforcement
    // of the performance design goal mentioned in the class-level comment.
    // ------------------------------------------------------------------------

    @Override
    public final void collect(T record) {
        throw new IllegalStateException(
                "This method should never be called. Since this wrapped method is called outside.");
    }

    @Override
    public final void collect(T record, long timestamp) {
        watermarkGenerator.onEvent(record, timestamp, onEventWatermarkOutput);
    }

    // ------------------------------------------------------------------------
    // WatermarkOutput Methods
    //
    // These two methods are final as well, to enforce the contract that the
    // watermarks from emitWatermark(Watermark) go to the same output as the
    // watermarks from the watermarkGenerator.onEvent(...) calls in the collect(...)
    // methods.
    // ------------------------------------------------------------------------

    @Override
    public final void emitWatermark(Watermark watermark) {
        onEventWatermarkOutput.emitWatermark(watermark);
    }

    @Override
    public final void markIdle() {
        onEventWatermarkOutput.markIdle();
    }

    public final void emitPeriodicWatermark() {
        watermarkGenerator.onPeriodicEmit(periodicWatermarkOutput);
    }

    // ------------------------------------------------------------------------
    // Factories
    // ------------------------------------------------------------------------

    /**
     * Creates a new SourceOutputWithWatermarks that emits records to the given DataOutput and
     * watermarks to the (possibly different) WatermarkOutput.
     */
    public static <E> SourceOutputWithWatermarks<E> createWithSameOutputs(
            PushingAsyncDataInput.DataOutput<E> recordsAndWatermarksOutput,
            WatermarkGenerator<E> watermarkGenerator) {

        final WatermarkOutput watermarkOutput =
                new WatermarkToDataOutput(recordsAndWatermarksOutput);

        return new SourceOutputWithWatermarks<>(
                watermarkGenerator, watermarkOutput, watermarkOutput);
    }

    /**
     * Creates a new SourceOutputWithWatermarks that emits records to the given DataOutput and
     * watermarks to the different WatermarkOutputs.
     */
    public static <E> SourceOutputWithWatermarks<E> createWithSeparateOutputs(
            WatermarkOutput onEventWatermarkOutput,
            WatermarkOutput periodicWatermarkOutput,
            WatermarkGenerator<E> watermarkGenerator) {

        return new SourceOutputWithWatermarks<>(
                watermarkGenerator, onEventWatermarkOutput, periodicWatermarkOutput);
    }
}
