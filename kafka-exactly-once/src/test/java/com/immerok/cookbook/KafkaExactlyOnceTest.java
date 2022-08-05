package com.immerok.cookbook;

import static com.immerok.cookbook.KafkaExactlyOnce.INPUT;
import static com.immerok.cookbook.KafkaExactlyOnce.OUTPUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.immerok.cookbook.events.StringSuppplier;
import com.immerok.cookbook.extensions.FlinkMiniClusterExtension;
import com.immerok.cookbook.utils.CookbookKafkaCluster;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Stream;
import net.mguenther.kafka.junit.ObserveKeyValues;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(FlinkMiniClusterExtension.class)
class KafkaExactlyOnceTest {

    /**
     * Runs the production job against an in-memory Kafka cluster.
     *
     * <p>This is a manual test because this job will never finish.
     */
    @Test
    @Disabled("Not running 'testProductionJob()' because it is a manual test.")
    void testProductionJob() throws Exception {
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopicAsync(INPUT, Stream.generate(new StringSuppplier()), false);
            kafka.createTopic(TopicConfig.withName(OUTPUT));

            KafkaExactlyOnce.runJob();
        }
    }

    @ParameterizedTest
    @MethodSource("exactlyOnceScenarios")
    void JobProducesExpectedNumberOfResults(
            String isolationLevel,
            int numRecordsToSend,
            boolean failTransactions,
            int numExpectedRecords)
            throws Exception {
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopic(
                    INPUT,
                    Stream.generate(new StringSuppplier()).limit(numRecordsToSend),
                    failTransactions);
            kafka.observe(
                    ObserveKeyValues.on(INPUT, numRecordsToSend)
                            .with(
                                    ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                                    IsolationLevel.READ_UNCOMMITTED
                                            .toString()
                                            .toLowerCase(Locale.ROOT)));

            var consumerProperties = new Properties();
            consumerProperties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);

            KafkaSource<String> source =
                    KafkaSource.<String>builder()
                            .setBootstrapServers("localhost:9092")
                            .setTopics(INPUT)
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setProperties(consumerProperties)
                            .setValueOnlyDeserializer(new SimpleStringSchema())
                            .build();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            KafkaExactlyOnce.defineWorkflow(env, source);
            JobClient jobClient = env.executeAsync();

            kafka.observe(ObserveKeyValues.on(OUTPUT, numExpectedRecords));
            assertThat(jobClient.cancel().cancel(true)).isTrue();
        }
    }

    static Stream<Arguments> exactlyOnceScenarios() {
        return Stream.of(
                /**
                 * When the transactions are successful, all the records are read, regardless of the
                 * isolation level.
                 */
                arguments("read_committed", 50, false, 50),
                arguments("read_uncommitted", 50, false, 50),
                /**
                 * When the transactions fail, Flink KafkaSource considers aborted data when the
                 * isolation level is read_uncommitted (ideal for E2E ALO), but ignores it when the
                 * isolation_level is read_committed (ideal for E2E EOS)
                 */
                arguments("read_uncommitted", 50, true, 50),
                arguments("read_committed", 50, true, 0));
    }
}
