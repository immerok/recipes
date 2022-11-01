package com.immerok.cookbook;

import static com.immerok.cookbook.SplitStream.TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

import com.immerok.cookbook.events.Event;
import com.immerok.cookbook.events.EventSupplier;
import com.immerok.cookbook.extensions.MiniClusterExtensionFactory;
import com.immerok.cookbook.utils.CookbookKafkaCluster;
import java.util.Iterator;
import java.util.stream.Stream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.PojoTestUtils;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SplitStreamTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK =
            MiniClusterExtensionFactory.withDefaultConfiguration();

    /**
     * Runs the production job against an in-memory Kafka cluster.
     *
     * <p>This is a manual test because this job will never finish.
     */
    @Test
    @Disabled("Not running 'testProductionJob()' because it is a manual test.")
    void testProductionJob() throws Exception {
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopicAsync(TOPIC, Stream.generate(new EventSupplier()));

            SplitStream.runJob();
        }
    }

    @Test
    void JobProducesAtLeastOneResult() throws Exception {

        final DataStream.Collector<Event> criticalEventSink = new DataStream.Collector<>();
        final DataStream.Collector<Event> majorEventSink = new DataStream.Collector<>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStreamSource<Event> source =
                env.fromElements(
                        new Event(1, "some critical event", Event.Priority.CRITICAL),
                        new Event(2, "some major event", Event.Priority.MAJOR),
                        new Event(3, "some minor event", Event.Priority.MINOR));

        SplitStream.defineWorkflow(
                source,
                workflow -> workflow.collectAsync(criticalEventSink),
                workflow -> workflow.collectAsync(majorEventSink));
        env.executeAsync();

        try (CloseableIterator<Event> criticalEvents = criticalEventSink.getOutput();
                CloseableIterator<Event> majorEvents = majorEventSink.getOutput()) {
            assertNotEmptyAndMatchPriority(criticalEvents, Event.Priority.CRITICAL);
            assertNotEmptyAndMatchPriority(majorEvents, Event.Priority.MAJOR);
        }
    }

    private static void assertNotEmptyAndMatchPriority(
            Iterator<Event> events, Event.Priority priority) {
        assertThat(events)
                .toIterable()
                .isNotEmpty()
                .allSatisfy(event -> assertThat(event.priority).isEqualTo(priority));
    }

    /** Verify that Flink recognizes the Event type as a POJO that it can serialize efficiently. */
    @Test
    void EventsAreAPOJOs() {
        PojoTestUtils.assertSerializedAsPojo(Event.class);
    }
}
