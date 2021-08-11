package org.apache.flink.streaming.examples.kafkatest;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.CollectionUtil;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Flink DataStream job for testing Kafka connectors on VVP. */
public class KafkaTestJob {

    private static final Logger LOG =
            LoggerFactory.getLogger(
                    org.apache.flink.streaming.examples.kafkatest.KafkaTestJob.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool parameters = ParameterTool.fromArgs(args);

//        String savepointPath = "/Users/liusenhong/Desktop/b7cb8bdeebdc0e5f7ec66648a1cf13d7/chk-24";
//        Configuration config = new Configuration();
//        config.setString(SavepointConfigOptions.SAVEPOINT_PATH, savepointPath);
//        config.setBoolean(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, false);
//        StreamExecutionEnvironment env =
//        StreamExecutionEnvironment.getExecutionEnvironment(config);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        String brokers = parameters.get("brokers");
        String inputTopic = parameters.get("input-topic");
        String outputTopic = parameters.get("output-topic");

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "vvp-datastream-e2e");

        DataStream<Tuple2<String, Integer>> source;

        LOG.info("Using FLIP-27 Kafka Source for testing");
        KafkaSource<Tuple2<String, Integer>> kafkaSource =
                KafkaSource.<Tuple2<String, Integer>>builder()
                        .setBootstrapServers(brokers)
                        .setTopics(inputTopic)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setDeserializer(
                                KafkaRecordDeserializationSchema.of(
                                        new KafkaMessageTuple2DeserializationSchema()))
                        .setBounded(OffsetsInitializer.latest())
                        .setGroupId(kafkaProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG))
                        .build();
        source =
                env.fromSource(kafkaSource, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source")
                        .uid("source");

        FlinkKafkaProducer<Tuple2<String, Integer>> kafkaSinkFunction =
                new FlinkKafkaProducer<>(
                        outputTopic,
                        new KafkaMessageTuple2SerializationSchema(outputTopic),
                        kafkaProperties,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        DataStream<Tuple2<String, Integer>> res =
                source.map(
                                new MapFunction<
                                        Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                                    @Override
                                    public Tuple2<String, Integer> map(
                                            Tuple2<String, Integer> value) throws Exception {
                                        return value;
                                    }
                                })
                        .reinterpretAsKeyedStream(value -> value.f0)
                        .sum(1)
                        .uid("sum");

        res.addSink(kafkaSinkFunction).name("Kafka SinkFunction").uid("sink").setParallelism(1);

        // start a checkpoint every 1000 ms
//        env.enableCheckpointing(1000);
//        //
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//
//        env.getCheckpointConfig().setCheckpointStorage("file:///Users/liusenhong/Desktop");
//
//        env.getCheckpointConfig().enableUnalignedCheckpoints();
        //        CheckpointRestoreByIDEUtils.run(env.getStreamGraph(), savepointPath);
        //        StreamGraph streamGraph = env.getStreamGraph();
        //
        // streamGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath));

        env.execute("Kafka DataStream E2E Test on VVP");
//        System.out.println(env.getExecutionPlan());
    }

    private static boolean isStr2Num(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static class MyMapFunction
            extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>
            implements CheckpointedFunction {
        ListState<KVState> storedState;
        List<KVState> state;

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> stringStringTuple2)
                throws Exception {

            KVState tmp = new KVState("", 0);
            for (KVState s : state) {
                if (s.name.equals(stringStringTuple2.f0)) {
                    tmp = s;
                }
            }

            tmp.val += stringStringTuple2.f1;
            if (stringStringTuple2.f0 != null && tmp.name.equals("")) {
                tmp.name = stringStringTuple2.f0;
                state.add(tmp);
            }

            stringStringTuple2.setField(tmp.val, 1);
            return stringStringTuple2;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext)
                throws Exception {
            KVState tmp = state.get(0);
            state.remove(0);
            state.add(tmp);
            storedState.update(state);
        }

        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext)
                throws Exception {
            storedState =
                    functionInitializationContext
                            .getOperatorStateStore()
                            .getListState(new ListStateDescriptor<KVState>("sum", KVState.class));
            if (functionInitializationContext.isRestored()) {
                state = new ArrayList<KVState>(CollectionUtil.iterableToList(storedState.get()));
            } else {
                state = new ArrayList<>();
            }
        }
    }

    private static class KVState {
        String name;
        int val;

        KVState(String name, int val) {
            this.name = name;
            this.val = val;
        }
    }

    private static class KafkaMessageTuple2SerializationSchema
            implements KafkaSerializationSchema<Tuple2<String, Integer>> {

        private final String sinkTopic;

        public KafkaMessageTuple2SerializationSchema(String sinkTopic) {
            checkNotNull(sinkTopic, "Topic must be specified in KafkaSerializationSchema");
            this.sinkTopic = sinkTopic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                Tuple2<String, Integer> tuple, @Nullable Long timestamp) {
            return new ProducerRecord<>(
                    sinkTopic,
                    0,
                    timestamp,
                    tuple.f0 == null ? null : tuple.f0.getBytes(StandardCharsets.UTF_8),
                    tuple.f1 == null
                            ? null
                            : String.valueOf(tuple.f1).getBytes(StandardCharsets.UTF_8));
        }
    }

    private static class KafkaMessageTuple2DeserializationSchema
            implements KafkaDeserializationSchema<Tuple2<String, Integer>> {

        @Override
        public boolean isEndOfStream(Tuple2<String, Integer> tuple) {
            return false;
        }

        @Override
        public Tuple2<String, Integer> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) {
            String key =
                    consumerRecord.key() == null
                            ? new String("Null")
                            : new String(consumerRecord.key());
            //            String key = new String(consumerRecord.topic() +
            // consumerRecord.partition());
            Integer value =
                    consumerRecord.value() == null
                            ? null
                            : Integer.parseInt(new String(consumerRecord.value()));
            return new Tuple2<>(key, value);
        }

        @Override
        public TypeInformation<Tuple2<String, Integer>> getProducedType() {
            return TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {});
        }
    }
}
