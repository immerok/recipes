package com.immerok.cookbook;

import static com.immerok.cookbook.LateDataToSeparateSink.TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

import com.immerok.cookbook.events.Event;
import com.immerok.cookbook.events.EventDeserializationSchema;
import com.immerok.cookbook.events.EventSupplier;
import com.immerok.cookbook.extensions.MiniClusterExtensionFactory;
import com.immerok.cookbook.utils.CookbookKafkaCluster;
import java.util.stream.Stream;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.PojoTestUtils;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class LateDataToSeparateSinkTest {

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
            kafka.createTopicAsync(TOPIC, Stream.generate(new EventSupplier(LATE_ELEMENT_DATA)));

            LateDataToSeparateSink.runJob();
        }
    }

    private static final String LATE_ELEMENT_DATA = "Eagles";

    @Test
    void JobProducesAtLeastOneResult() throws Exception {
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {

            kafka.createTopicAsync(TOPIC, Stream.generate(new EventSupplier(LATE_ELEMENT_DATA)));

            KafkaSource<Event> source =
                    KafkaSource.<Event>builder()
                            .setBootstrapServers("localhost:9092")
                            .setTopics(TOPIC)
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setValueOnlyDeserializer(new EventDeserializationSchema())
                            .build();

            final DataStream.Collector<Event> mainTestSink = new DataStream.Collector<>();
            final DataStream.Collector<Event> lateTestSink = new DataStream.Collector<>();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            LateDataToSeparateSink.defineWorkflow(
                    env,
                    source,
                    mainWorkflowOutput -> mainWorkflowOutput.collectAsync(mainTestSink),
                    lateWorkflowInput -> lateWorkflowInput.collectAsync(lateTestSink));
            env.executeAsync();

            try (final CloseableIterator<Event> mainEvents = mainTestSink.getOutput();
                    final CloseableIterator<Event> lateEvents = lateTestSink.getOutput()) {

                // verify that all late elements ended up in the lateElements stream
                for (int x = 0; x < 100; x++) {
                    assertThat(mainEvents.hasNext()).isTrue();
                    assertThat(mainEvents.next().data).doesNotContain(LATE_ELEMENT_DATA);
                }
                for (int x = 0; x < 10; x++) {
                    assertThat(lateEvents.hasNext()).isTrue();
                    assertThat(lateEvents.next().data).isEqualTo(LATE_ELEMENT_DATA);
                }
            }
        }
    }

    /** Verify that Flink recognizes the Event type as a POJO that it can serialize efficiently. */
    @Test
    void EventsAreAPOJOs() {
        PojoTestUtils.assertSerializedAsPojo(Event.class);
    }
}
