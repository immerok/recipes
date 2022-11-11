package com.immerok.cookbook;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.immerok.cookbook.events.Event;
import java.time.Instant;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.PojoTestUtils;
import org.junit.jupiter.api.Test;

public class MeasuringLatencyTest {

    /** Verify that the pipeline runs and passes events through to the sink. */
    @Test
    void testPipelinePassesEventsThrough() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Event testInput = new Event(0, "", Instant.EPOCH);
        DataStream<Event> inputStream = env.fromElements(testInput);

        final DataStream.Collector<Event> testSink = new DataStream.Collector<>();

        MeasuringLatency.defineWorkflow(
                inputStream, workflow -> workflow.collectAsync(testSink), 10_000);

        env.executeAsync();

        assertThat(testSink.getOutput()).toIterable().containsExactly(testInput);
    }

    /** Verify that Flink recognizes the Event type as a POJO that it can serialize efficiently. */
    @Test
    void testEventsArePojosThatAvoidUsingKyro() {
        PojoTestUtils.assertSerializedAsPojoWithoutKryo(Event.class);
    }
}
