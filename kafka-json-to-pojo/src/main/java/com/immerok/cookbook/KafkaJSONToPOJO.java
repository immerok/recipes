package com.immerok.cookbook;

import com.immerok.cookbook.events.Event;
import com.immerok.cookbook.events.EventDeserializationSchema;
import java.util.function.Consumer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;

public class KafkaJSONToPOJO {

    static final String TOPIC = "input";

    public static void main(String[] args) throws Exception {
        runJob();
    }

    static void runJob() throws Exception {
        KafkaSource<Event> source =
                KafkaSource.<Event>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics(TOPIC)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new EventDeserializationSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        defineWorkflow(env, source, workflow -> workflow.sinkTo(new PrintSink<>()));
        env.execute();
    }

    static void defineWorkflow(
            StreamExecutionEnvironment env,
            Source<Event, ?, ?> source,
            Consumer<DataStream<Event>> sinkApplier) {
        final DataStreamSource<Event> kafka =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka");

        // additional workflow steps go here

        sinkApplier.accept(kafka);
    }
}
