package com.immerok.cookbook

import com.immerok.cookbook.events.StringSupplier
import com.immerok.cookbook.extensions.FlinkMiniClusterExtension
import com.immerok.cookbook.utils.CookbookKafkaCluster
import com.immerok.cookbook.utils.DataStreamCollectUtil
import com.immerok.cookbook.utils.DataStreamCollector
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.util.stream.Stream

@ExtendWith(FlinkMiniClusterExtension::class)
internal class WordCountTest {

    /**
     * Runs the production job against an in-memory Kafka cluster.
     *
     * This is a manual test because this job will never finish.
     */
    @Test
    @Disabled("Not running 'testProductionJob()' because it is a manual test.")
    fun testProductionJob() {
        CookbookKafkaCluster().use { kafka ->
            kafka.createTopicAsync(TOPIC, Stream.generate(StringSupplier()))

            runJob()
        }
    }

    @Test
    fun testJobProducesAtLeastOneResult() {
        CookbookKafkaCluster().use { kafka ->
            kafka.createTopicAsync(TOPIC, Stream.generate(StringSupplier()))

            val source = KafkaSource.builder<String>()
                .setBootstrapServers("localhost:9092")
                .setTopics(TOPIC)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(SimpleStringSchema())
                // set an upper bound so that the job (and this test) will end
                .setBounded(OffsetsInitializer.latest())
                .build()

            // the kafka cluster only creates a single partition
            // a higher parallelism would cause the job to produce no output,
            // because the job isn't configuring an idle timeout
            val sourceParallelism = 1

            val dataStreamCollector = DataStreamCollectUtil()

            val testSink = DataStreamCollector<Event>()

            val env = StreamExecutionEnvironment.getExecutionEnvironment()
            defineWorkflow(
                env,
                source,
                sourceParallelism
            ) { workflow -> dataStreamCollector.collectAsync(workflow, testSink) }
            val jobClient = env.executeAsync()
            dataStreamCollector.startCollect(jobClient)

            assertThat(testSink.output).toIterable().isNotEmpty
        }
    }
}
