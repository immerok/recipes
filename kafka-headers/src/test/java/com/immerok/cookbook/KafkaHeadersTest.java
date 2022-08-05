package com.immerok.cookbook;

import static com.immerok.cookbook.KafkaHeaders.TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

import com.immerok.cookbook.events.EnrichedEvent;
import com.immerok.cookbook.events.EventSupplier;
import com.immerok.cookbook.events.HeaderGenerator;
import com.immerok.cookbook.events.KafkaHeadersEventDeserializationSchema;
import com.immerok.cookbook.extensions.FlinkMiniClusterExtension;
import com.immerok.cookbook.utils.CookbookKafkaCluster;
import com.immerok.cookbook.utils.DataStreamCollectUtil;
import com.immerok.cookbook.utils.DataStreamCollector;
import java.util.stream.Stream;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(FlinkMiniClusterExtension.class)
class KafkaHeadersTest {
    /**
     * Runs the production job against an in-memory Kafka cluster.
     *
     * <p>This is a manual test because this job will never finish.
     */
    @Test
    @Disabled("Not running 'testProductionJob()' because it is a manual test.")
    void testProductionJob() throws Exception {
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopicAsync(
                    TOPIC, Stream.generate(new EventSupplier()), new HeaderGenerator());

            KafkaHeaders.runJob();
        }
    }

    @Test
    void JobProducesAtLeastOneResult() throws Exception {
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopicAsync(
                    TOPIC, Stream.generate(new EventSupplier()), new HeaderGenerator());

            KafkaSource<EnrichedEvent> source =
                    KafkaSource.<EnrichedEvent>builder()
                            .setBootstrapServers("localhost:9092")
                            .setTopics(TOPIC)
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            // set an upper bound so that the job (and this test) will end
                            .setBounded(OffsetsInitializer.latest())
                            .setDeserializer(new KafkaHeadersEventDeserializationSchema())
                            .build();

            final DataStreamCollectUtil dataStreamCollector = new DataStreamCollectUtil();

            final DataStreamCollector<EnrichedEvent> testSink = new DataStreamCollector<>();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            KafkaHeaders.defineWorkflow(
                    env, source, workflow -> dataStreamCollector.collectAsync(workflow, testSink));
            dataStreamCollector.startCollect(env.executeAsync());

            assertThat(testSink.getOutput()).toIterable().isNotEmpty();
        }
    }

    /**
     * Verify that Flink recognizes the EnrichedEvent type as a POJO that it can serialize
     * efficiently.
     */
    @Test
    void EventsAreAPOJOs() {
        TypeSerializer<EnrichedEvent> eventSerializer =
                TypeInformation.of(EnrichedEvent.class).createSerializer(new ExecutionConfig());

        assertThat(eventSerializer).isInstanceOf(PojoSerializer.class);
    }
}
