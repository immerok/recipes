package com.immerok.cookbook;

import static com.immerok.cookbook.CompiledPlanRecipe.TRANSACTION_TOPIC;
import static com.immerok.cookbook.CompiledPlanRecipe.printSinkDDL;
import static com.immerok.cookbook.CompiledPlanRecipe.streamingDeduplication;
import static com.immerok.cookbook.CompiledPlanRecipe.transactionsDDL;

import com.immerok.cookbook.extensions.MiniClusterExtensionFactory;
import com.immerok.cookbook.records.DuplicatingTransactionSupplier;
import com.immerok.cookbook.utils.CookbookKafkaCluster;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class CompiledPlanRecipeTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK =
            MiniClusterExtensionFactory.withDefaultConfiguration();

    /**
     * Set an explicit path here if you want the plan file to be available after running these tests
     */
    private static Path planLocation;

    @BeforeAll
    public static void setPlanLocation() {
        if (planLocation == null) {
            try {
                planLocation = Files.createTempFile("plan", ".json");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Creates a compiled JSON plan file for a streaming Table application. Note that the Kafka
     * cluster and relevant topics don't need to exist when this code is run -- Kafka is only
     * necessary later when the plan is executed.
     */
    @Test
    @Order(1)
    public void compileAndWritePlan() {
        CompiledPlanRecipe.compileAndWritePlan(planLocation);
    }

    /**
     * Loads and executes the compiled json plan written out by compileAndWritePlan(), which needs
     * to be run first.
     */
    @Test
    @Order(2)
    public void loadAndExecutePlan() throws InterruptedException {

        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            final int numberOfDuplicatedTransactions = 10;

            // Create and serve a bounded stream of Transactions
            kafka.createTopic(
                    TRANSACTION_TOPIC,
                    Stream.generate(new DuplicatingTransactionSupplier())
                            .limit(numberOfDuplicatedTransactions));

            // Start executing the job (asynchronously)
            TableResult execution = CompiledPlanRecipe.runCompiledPlan(planLocation);

            // Wait and watch as the results are printed out
            final JobClient jobClient = execution.getJobClient().get();
            try {
                Thread.sleep(1_000 * 5);
            } finally {
                jobClient.cancel();
            }
        }
    }

    @Test
    public void printPlan() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql(transactionsDDL);
        tableEnv.executeSql(printSinkDDL);

        tableEnv.compilePlanSql(streamingDeduplication).printJsonString();
    }

    /**
     * Runs the original job against an in-memory Kafka cluster.
     *
     * <p>This is a manual test because this job will never finish.
     */
    @Test
    @Disabled("Not running 'testOriginalJob()' because it is a manual test that never finishes.")
    void testOriginalJob() throws Exception {
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopicAsync(
                    TRANSACTION_TOPIC, Stream.generate(new DuplicatingTransactionSupplier()));

            TableResult result = CompiledPlanRecipe.runOriginalJob();
            result.await();
        }
    }
}
