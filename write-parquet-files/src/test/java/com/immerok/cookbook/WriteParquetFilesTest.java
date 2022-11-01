package com.immerok.cookbook;

import static com.immerok.cookbook.WriteParquetFiles.TRANSACTION_TOPIC;
import static com.immerok.cookbook.WriteParquetFiles.createFilesystemConnectorDescriptor;

import com.immerok.cookbook.extensions.FlinkMiniClusterExtension;
import com.immerok.cookbook.records.Transaction;
import com.immerok.cookbook.records.TransactionDeserializer;
import com.immerok.cookbook.records.TransactionSupplier;
import com.immerok.cookbook.records.TransactionTestDataSupplier;
import com.immerok.cookbook.utils.CookbookKafkaCluster;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class WriteParquetFilesTest {
    @RegisterExtension
    static final FlinkMiniClusterExtension FLINK =
            new FlinkMiniClusterExtension(
                    new Configuration()
                            .set(
                                    CoreOptions.DEFAULT_PARALLELISM,
                                    FlinkMiniClusterExtension.DEFAULT_PARALLELISM / 2));

    /**
     * Runs the production job against an in-memory Kafka cluster.
     *
     * <p>This is a manual test because this job will never finish.
     */
    @Test
    @Disabled("Not running 'testProductionJob()' because it is a manual test.")
    void testProductionJob(@TempDir java.nio.file.Path tmpDir) {
        final Path path = new Path(tmpDir.toUri());

        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopicAsync(TRANSACTION_TOPIC, Stream.generate(new TransactionSupplier()));

            WriteParquetFiles.runJob(path);

            final TableEnvironment tEnv =
                    TableEnvironment.create(EnvironmentSettings.inStreamingMode());
            tEnv.createTable(
                    "FileSource",
                    createFilesystemConnectorDescriptor(path)
                            .option("source.monitor-interval", "1")
                            .build());

            tEnv.executeSql("select * from FileSource").print();
        }
    }

    @Test
    void testWrittenParquetOutput(@TempDir java.nio.file.Path tmpDir) throws Exception {
        final Path path = new Path(tmpDir.toUri());

        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopic(TRANSACTION_TOPIC, TransactionTestDataSupplier.getTestData());

            KafkaSource<Transaction> transactionSource =
                    KafkaSource.<Transaction>builder()
                            .setBootstrapServers(kafka.getBrokerList())
                            .setTopics(TRANSACTION_TOPIC)
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            // set an upper bound so that the job (and this test) will end
                            .setBounded(OffsetsInitializer.latest())
                            .setValueOnlyDeserializer(new TransactionDeserializer())
                            .build();

            WriteParquetFiles.runJob(
                    path,
                    transactionSource,
                    result -> {
                        try {
                            // wait until the writing is complete
                            result.await();
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    },
                    EnvironmentSettings.inBatchMode());
        }

        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTable("FileSource", createFilesystemConnectorDescriptor(path).build());

        try (CloseableIterator<Row> collected =
                tEnv.executeSql("select * from FileSource").collect()) {

            Assertions.assertThat(collected)
                    .toIterable()
                    .map(Row::toString)
                    .containsExactlyInAnyOrder(
                            "+I[2021-10-08 12:33:12.000000000, null, 1, 12, 325.12]",
                            "+I[2021-10-14 17:04:00.000000000, null, 3, 12, 52.48]",
                            "+I[2021-10-14 17:06:00.000000000, null, 4, 32, 26.11]",
                            "+I[2021-10-14 18:23:00.000000000, null, 5, 32, 22.03]",
                            "+I[2021-10-10 08:00:00.000000000, null, 2, 7, 13.99]",
                            "+I[2021-10-10 08:00:00.000000000, null, 2, 7, 13.99]");
        }
    }
}
