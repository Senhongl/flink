package org.apache.flink.streaming.examples.kafkatest;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Flink DataStream job for testing Kafka connectors on VVP. */
public class KafkaTestJob {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTestJob.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool parameters = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // The order of messages will be validated, so we just use 1 parallelism in the job
        env.setParallelism(1);

        String brokers = parameters.get("brokers");
        String inputTopic = parameters.get("input-topic");
        String outputTopic = parameters.get("output-topic");

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "vvp-datastream-e2e");

        DataStreamSource<Tuple2<String, String>> source;

        if (parameters.has("use-new-api")) {
            LOG.info("Using FLIP-27 Kafka Source for testing");
            KafkaSource<Tuple2<String, String>> kafkaSource =
                    KafkaSource.<Tuple2<String, String>>builder()
                            .setBootstrapServers(brokers)
                            .setTopics(inputTopic)
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setDeserializer(
                                    KafkaRecordDeserializationSchema.of(
                                            new KafkaMessageTuple2DeserializationSchema()))
                            .setGroupId(kafkaProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG))
                            .build();
            source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        } else {
            LOG.info("Using legacy Kafka SourceFunction for testing");
            FlinkKafkaConsumer<Tuple2<String, String>> kafkaSourceFunction =
                    new FlinkKafkaConsumer<>(
                            inputTopic,
                            new KafkaMessageTuple2DeserializationSchema(),
                            kafkaProperties);
            kafkaSourceFunction.setStartFromEarliest();
            source = env.addSource(kafkaSourceFunction, "Kafka SourceFunction");
        }

        FlinkKafkaProducer<Tuple2<String, String>> kafkaSinkFunction =
                new FlinkKafkaProducer<>(
                        outputTopic,
                        new KafkaMessageTuple2SerializationSchema(outputTopic),
                        kafkaProperties,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        source.addSink(kafkaSinkFunction).name("Kafka SinkFunction");

        env.enableCheckpointing(10 * 1000);
        env.execute("Kafka DataStream E2E Test on VVP");
    }

    private static class KafkaMessageTuple2SerializationSchema
            implements KafkaSerializationSchema<Tuple2<String, String>> {

        private final String sinkTopic;

        public KafkaMessageTuple2SerializationSchema(String sinkTopic) {
            checkNotNull(sinkTopic, "Topic must be specified in KafkaSerializationSchema");
            this.sinkTopic = sinkTopic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                Tuple2<String, String> tuple, @Nullable Long timestamp) {
            return new ProducerRecord<>(
                    sinkTopic,
                    0,
                    timestamp,
                    tuple.f0 == null ? null : tuple.f0.getBytes(StandardCharsets.UTF_8),
                    tuple.f1 == null ? null : tuple.f1.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static class KafkaMessageTuple2DeserializationSchema
            implements KafkaDeserializationSchema<Tuple2<String, String>> {

        @Override
        public boolean isEndOfStream(Tuple2<String, String> tuple) {
            return false;
        }

        @Override
        public Tuple2<String, String> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) {
            String key = consumerRecord.key() == null ? null : new String(consumerRecord.key());
            String value =
                    consumerRecord.value() == null ? null : new String(consumerRecord.value());
            return new Tuple2<>(key, value);
        }

        @Override
        public TypeInformation<Tuple2<String, String>> getProducedType() {
            return TypeInformation.of(new TypeHint<Tuple2<String, String>>() {});
        }
    }
}
