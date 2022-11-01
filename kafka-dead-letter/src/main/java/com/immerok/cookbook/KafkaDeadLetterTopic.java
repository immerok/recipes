package com.immerok.cookbook;

import com.immerok.cookbook.events.Event;
import com.immerok.cookbook.events.JsonDeserialization;
import java.io.IOException;
import java.util.function.Consumer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class KafkaDeadLetterTopic {

    static final String TOPIC = "input";

    public static void main(String[] args) throws Exception {
        runJob();
    }

    static void runJob() throws Exception {
        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics(TOPIC)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        defineWorkflow(
                env,
                source,
                workflow -> workflow.sinkTo(new PrintSink<>()),
                errors -> errors.sinkTo(new PrintSink<>("Errors")));
        env.execute();
    }

    static void defineWorkflow(
            StreamExecutionEnvironment env,
            Source<String, ?, ?> source,
            Consumer<DataStream<Event>> sinkApplier,
            Consumer<DataStream<String>> errorSinkApplier) {
        final OutputTag<String> deserializationErrors = new OutputTag<>("errors") {};

        final SingleOutputStreamOperator<Event> kafka =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka")
                        .process(
                                new ProcessFunction<>() {
                                    @Override
                                    public void processElement(
                                            String value,
                                            ProcessFunction<String, Event>.Context ctx,
                                            Collector<Event> out) {
                                        final Event deserialized;
                                        try {
                                            deserialized = JsonDeserialization.deserialize(value);
                                        } catch (IOException e) {
                                            ctx.output(deserializationErrors, value);
                                            return;
                                        }
                                        out.collect(deserialized);
                                    }
                                });

        final DataStream<String> elementsFailingDeserialization =
                kafka.getSideOutput(deserializationErrors);

        // Since we aren't deserializing events in the source we have
        // to apply the watermark strategy afterwards.
        // Depending on the source this may not be required if the timestamp
        // can be determined without deserializing the event.
        final SingleOutputStreamOperator<Event> eventsWithTimestampsAndWatermarks =
                kafka.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        (event, timestamp) -> event.timestamp.toEpochMilli()));

        // additional workflow / error-processing steps go here

        sinkApplier.accept(eventsWithTimestampsAndWatermarks);
        errorSinkApplier.accept(elementsFailingDeserialization);
    }
}
