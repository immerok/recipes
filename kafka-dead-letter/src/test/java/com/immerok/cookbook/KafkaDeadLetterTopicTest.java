package com.immerok.cookbook;

import static com.immerok.cookbook.KafkaDeadLetterTopic.TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

import com.immerok.cookbook.events.Event;
import com.immerok.cookbook.events.EventSupplier;
import com.immerok.cookbook.extensions.MiniClusterExtensionFactory;
import com.immerok.cookbook.utils.CookbookKafkaCluster;
import java.util.stream.Stream;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.PojoTestUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class KafkaDeadLetterTopicTest {

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

            KafkaDeadLetterTopic.runJob();
        }
    }

    @Test
    void JobProducesAtLeastOneResult() throws Exception {
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopic(TOPIC, Stream.generate(new EventSupplier()).limit(100));

            KafkaSource<String> source =
                    KafkaSource.<String>builder()
                            .setBootstrapServers("localhost:9092")
                            .setTopics(TOPIC)
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            // set an upper bound so that the job (and this test) will end
                            .setBounded(OffsetsInitializer.latest())
                            .setValueOnlyDeserializer(new SimpleStringSchema())
                            .build();

            final DataStream.Collector<Event> testSink = new DataStream.Collector<>();
            final DataStream.Collector<String> testErrorSink = new DataStream.Collector<>();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            KafkaDeadLetterTopic.defineWorkflow(
                    env,
                    source,
                    workflow -> workflow.collectAsync(testSink),
                    errors -> errors.collectAsync(testErrorSink));
            env.executeAsync();

            assertThat(testSink.getOutput()).toIterable().isNotEmpty();
            assertThat(testErrorSink.getOutput()).toIterable().isNotEmpty();
        }
    }

    /** Verify that Flink recognizes the Event type as a POJO that it can serialize efficiently. */
    @Test
    void EventsAreAPOJOs() {
        PojoTestUtils.assertSerializedAsPojo(Event.class);
    }
}
