package com.immerok.cookbook;

import com.immerok.cookbook.events.Event;
import java.time.Instant;
import java.util.function.Consumer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;

public class MeasuringLatency {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // replace fromElements with your production event source
        DataStream<Event> inputStream = env.fromElements(new Event(0, "", Instant.EPOCH));

        // replace the PrintSink with your production sink
        MeasuringLatency.defineWorkflow(
                inputStream, workflow -> workflow.sinkTo(new PrintSink<>()), 10_000);

        env.execute();
    }

    static void defineWorkflow(
            DataStream<Event> inputStream,
            Consumer<DataStream<Event>> sinkApplier,
            int eventTimeLagRingBufferSize) {

        final DataStream<Event> delayed = inputStream.map(new AddLatency());
        final DataStream<Event> measured =
                delayed.map(new MeasureLatency(eventTimeLagRingBufferSize));

        sinkApplier.accept(measured);
    }

    private static class AddLatency implements MapFunction<Event, Event> {

        @Override
        public Event map(Event event) throws Exception {
            // cause some additional latency
            Thread.sleep(100);

            // pass the event through (unmodified)
            return event;
        }
    }

    private static class MeasureLatency extends RichMapFunction<Event, Event> {
        private transient DescriptiveStatisticsHistogram eventTimeLag;

        // how many recent samples to use for computing the histogram statistics
        private final int eventTimeLagRingBufferSize;

        public MeasureLatency(int eventTimeLagRingBufferSize) {
            this.eventTimeLagRingBufferSize = eventTimeLagRingBufferSize;
        }

        @Override
        public Event map(Event event) {
            // measure and report the latency
            eventTimeLag.update(System.currentTimeMillis() - event.timestamp.toEpochMilli());

            // pass the event through (unmodified)
            return event;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // define a custom histogram metric for measuring latency
            eventTimeLag =
                    getRuntimeContext()
                            .getMetricGroup()
                            .histogram(
                                    "eventTimeLag",
                                    new DescriptiveStatisticsHistogram(eventTimeLagRingBufferSize));
        }
    }
}
