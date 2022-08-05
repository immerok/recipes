package com.immerok.cookbook;

import java.util.Locale;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.IsolationLevel;

public class KafkaExactlyOnce {

    static final String INPUT = "input";
    static final String OUTPUT = "output";

    public static void main(String[] args) throws Exception {
        runJob();
    }

    static void runJob() throws Exception {
        var consumerProperties = new Properties();
        consumerProperties.setProperty(
                ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT));

        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics(INPUT)
                        .setGroupId("KafkaExactlyOnceRecipeApplication")
                        .setStartingOffsets(
                                OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                        .setProperties(consumerProperties)
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        defineWorkflow(env, source);
        env.execute();
    }

    static void defineWorkflow(StreamExecutionEnvironment env, Source<String, ?, ?> source) {
        env.enableCheckpointing(10000);

        var producerProperties = new Properties();
        producerProperties.setProperty("transaction.timeout.ms", "60000");

        KafkaRecordSerializationSchema<String> serializer =
                KafkaRecordSerializationSchema.builder()
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setTopic(OUTPUT)
                        .build();

        KafkaSink<String> sink =
                KafkaSink.<String>builder()
                        .setBootstrapServers("localhost:9092")
                        .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionalIdPrefix("KafkaExactlyOnceRecipe")
                        .setKafkaProducerConfig(producerProperties)
                        .setRecordSerializer(serializer)
                        .build();

        final DataStream<String> text =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaExactlyOnce");

        text.sinkTo(sink);
    }
}
