package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkOutputMultiplexer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Implementation of {@link TimestampsAndWatermarks}, this class would not take responsibility of
 * extracting timestamp from records but only generating and propagating watermarks. The
 * {@link ReaderOutputAdaptor} would help extracting timestamp from the records.
 */
public class TimestampsAndWatermarskImpl<T> implements TimestampsAndWatermarks<T> {

    private final WatermarkGeneratorSupplier<T> watermarksFactory;

    private final WatermarkGeneratorSupplier.Context watermarksContext;

    private final ProcessingTimeService timeService;

    private final long periodicWatermarkInterval;

    private SplitLocalOutputs<T> currentPerSplitOutputs;

    private StreamingReaderOutput<T> currentMainOutput;

    private ScheduledFuture<?> periodicEmitHandle;

    public TimestampsAndWatermarskImpl(
            WatermarkStrategy<T> watermarkStrategy,
            MetricGroup metrics,
            ProcessingTimeService timeService,
            long periodicWatermarkInterval) {

        final TimestampsAndWatermarksContext context = new TimestampsAndWatermarksContext(metrics);

        this.watermarksFactory = watermarkStrategy;
        this.watermarksContext = context;
        this.timeService = timeService;
        this.periodicWatermarkInterval = periodicWatermarkInterval;
    }

    @Override
    public ReaderOutput<T> createMainOutput(PushingAsyncDataInput.DataOutput<T> output) {
        // At the moment, we assume only one output is ever created!
        // This assumption is strict, currently, because many of the classes in this
        // implementation
        // do not
        // support re-assigning the underlying output
        checkState(
                currentMainOutput == null && currentPerSplitOutputs == null,
                "already created a main output");

        final WatermarkOutput watermarkOutput = new WatermarkToDataOutput(output);
        final WatermarkGenerator<T> watermarkGenerator =
                watermarksFactory.createWatermarkGenerator(watermarksContext);

        currentPerSplitOutputs =
                new SplitLocalOutputs<>(
                        watermarkOutput,
                        watermarksFactory,
                        watermarksContext);

        currentMainOutput =
                new StreamingReaderOutput<>(
                        watermarkOutput,
                        watermarkGenerator,
                        currentPerSplitOutputs);


        return currentMainOutput;
    }

    @Override
    public void startPeriodicWatermarkEmits() {
        checkState(periodicEmitHandle == null, "periodic emitter already started");

        if (periodicWatermarkInterval == 0) {
            // a value of zero means not activated
            return;
        }

        periodicEmitHandle =
                timeService.scheduleWithFixedDelay(
                        this::triggerPeriodicEmit,
                        periodicWatermarkInterval,
                        periodicWatermarkInterval);
    }

    @Override
    public void stopPeriodicWatermarkEmits() {
        if (periodicEmitHandle != null) {
            periodicEmitHandle.cancel(false);
            periodicEmitHandle = null;
        }
    }

    void triggerPeriodicEmit(@SuppressWarnings("unused") long wallClockTimestamp) {
        if (currentPerSplitOutputs != null) {
            currentPerSplitOutputs.emitPeriodicWatermark();
        }
        if (currentMainOutput != null) {
            currentMainOutput.emitPeriodicWatermark();
        }
    }

    // ------------------------------------------------------------------------

    private static final class StreamingReaderOutput<T> extends SourceOutputWithWatermarks<T>
            implements ReaderOutput<T> {

        private final SplitLocalOutputs<T> splitLocalOutputs;

        StreamingReaderOutput(
                WatermarkOutput watermarkOutput,
                WatermarkGenerator<T> watermarkGenerator,
                SplitLocalOutputs<T> splitLocalOutputs) {

            super(
                    watermarkGenerator,
                    watermarkOutput,
                    watermarkOutput);
            this.splitLocalOutputs = splitLocalOutputs;;
        }

        @Override
        public SourceOutput<T> createOutputForSplit(String splitId) {
            return splitLocalOutputs.createOutputForSplit(splitId);
        }

        @Override
        public void releaseOutputForSplit(String splitId) {
            splitLocalOutputs.releaseOutputForSplit(splitId);
        }
    }

    // ------------------------------------------------------------------------

    /**
     * A holder and factory for split-local {@link SourceOutput}s. The split-local outputs maintain
     * local watermark generators with their own state, to facilitate per-split watermarking logic.
     *
     * @param <T> The type of the emitted records.
     */
    private static final class SplitLocalOutputs<T> {

        private final WatermarkOutputMultiplexer watermarkMultiplexer;
        private final Map<String, SourceOutputWithWatermarks<T>> localOutputs;
        private final WatermarkGeneratorSupplier<T> watermarksFactory;
        private final WatermarkGeneratorSupplier.Context watermarkContext;

        private SplitLocalOutputs(
                WatermarkOutput watermarkOutput,
                WatermarkGeneratorSupplier<T> watermarksFactory,
                WatermarkGeneratorSupplier.Context watermarkContext) {

            this.watermarksFactory = watermarksFactory;
            this.watermarkContext = watermarkContext;

            this.watermarkMultiplexer = new WatermarkOutputMultiplexer(watermarkOutput);
            this.localOutputs =
                    new LinkedHashMap<>(); // we use a LinkedHashMap because it iterates faster
        }

        SourceOutput<T> createOutputForSplit(String splitId) {
            final SourceOutputWithWatermarks<T> previous = localOutputs.get(splitId);
            if (previous != null) {
                return previous;
            }

            watermarkMultiplexer.registerNewOutput(splitId);
            final WatermarkOutput onEventOutput = watermarkMultiplexer.getImmediateOutput(splitId);
            final WatermarkOutput periodicOutput = watermarkMultiplexer.getDeferredOutput(splitId);

            final WatermarkGenerator<T> watermarks =
                    watermarksFactory.createWatermarkGenerator(watermarkContext);

            final SourceOutputWithWatermarks<T> localOutput =
                    SourceOutputWithWatermarks.createWithSeparateOutputs(
                            onEventOutput,
                            periodicOutput,
                            watermarks);

            localOutputs.put(splitId, localOutput);
            return localOutput;
        }

        void releaseOutputForSplit(String splitId) {
            localOutputs.remove(splitId);
            watermarkMultiplexer.unregisterOutput(splitId);
        }

        void emitPeriodicWatermark() {
            // The call in the loop only records the next watermark candidate for each local output.
            // The call to 'watermarkMultiplexer.onPeriodicEmit()' actually merges the watermarks.
            // That way, we save inefficient repeated merging of (partially outdated) watermarks
            // before
            // all local generators have emitted their candidates.
            for (SourceOutputWithWatermarks<?> output : localOutputs.values()) {
                output.emitPeriodicWatermark();
            }
            watermarkMultiplexer.onPeriodicEmit();
        }
    }
}
