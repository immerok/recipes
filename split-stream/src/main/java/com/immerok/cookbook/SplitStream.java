package com.immerok.cookbook;

import static java.util.Map.entry;

import com.immerok.cookbook.events.Event;
import com.immerok.cookbook.events.EventDeserializationSchema;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SplitStream {

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
        defineWorkflow(
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka"),
                workflow -> workflow.sinkTo(new PrintSink<>(Event.Priority.CRITICAL.name())),
                workflow -> workflow.sinkTo(new PrintSink<>(Event.Priority.MAJOR.name())));
        env.execute();
    }

    static void defineWorkflow(
            DataStream<Event> source,
            Consumer<DataStream<Event>> criticalEventSinkApplier,
            Consumer<DataStream<Event>> majorEventSinkApplier) {
        final Map<Event.Priority, OutputTag<Event>> tagsByPriority =
                Map.ofEntries(
                        entry(Event.Priority.CRITICAL, new OutputTag<>("critical") {}),
                        entry(Event.Priority.MAJOR, new OutputTag<>("major") {}),
                        entry(Event.Priority.MINOR, new OutputTag<>("minor") {}));

        final SingleOutputStreamOperator<Event> process =
                source.process(
                        new ProcessFunction<>() {
                            @Override
                            public void processElement(
                                    Event value,
                                    ProcessFunction<Event, Event>.Context ctx,
                                    Collector<Event> out) {
                                final OutputTag<Event> selectedOutput =
                                        tagsByPriority.get(value.priority);
                                ctx.output(selectedOutput, value);
                            }
                        });

        final DataStream<Event> criticalEvents =
                process.getSideOutput(tagsByPriority.get(Event.Priority.CRITICAL));

        final DataStream<Event> majorEvents =
                process.getSideOutput(tagsByPriority.get(Event.Priority.MAJOR));

        // we are not required to consume all splits (e.g., for MINOR events)

        // additional workflow steps go here

        criticalEventSinkApplier.accept(criticalEvents);
        majorEventSinkApplier.accept(majorEvents);
    }
}
