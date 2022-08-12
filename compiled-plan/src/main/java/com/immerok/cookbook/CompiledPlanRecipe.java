package com.immerok.cookbook;

import java.nio.file.Path;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class CompiledPlanRecipe {

    static final String TRANSACTION_TOPIC = "transactions";

    static final String transactionsDDL =
            String.format(
                    String.join(
                            "\n",
                            "CREATE TABLE Transactions (",
                            "  t_time TIMESTAMP_LTZ(3),",
                            "  t_id BIGINT,",
                            "  t_customer_id BIGINT,",
                            "  t_amount DECIMAL",
                            ") WITH (",
                            "  'connector' = 'kafka',",
                            "  'topic' = '%s',",
                            "  'properties.bootstrap.servers' = 'localhost:9092',",
                            "  'scan.startup.mode' = 'earliest-offset',",
                            "  'format' = 'json',",
                            "  'json.timestamp-format.standard' = 'ISO-8601'",
                            ")"),
                    TRANSACTION_TOPIC);

    public static final String printSinkDDL =
            String.join(
                    "\n",
                    "CREATE TABLE print_sink (",
                    "  t_id BIGINT,",
                    "  t_customer_id BIGINT,",
                    "  t_amount DECIMAL(5, 2)",
                    ") WITH (",
                    "  'connector' = 'print'",
                    ")");

    public static final String streamingDeduplication =
            String.join(
                    "\n",
                    "INSERT INTO print_sink",
                    "SELECT t_id, t_customer_id, t_amount",
                    "FROM (",
                    "  SELECT *,",
                    "  ROW_NUMBER() OVER (PARTITION BY t_id ORDER BY t_time ASC) AS row_num",
                    "  FROM Transactions",
                    ") WHERE row_num = 1");

    public static void main(String[] args) {
        runOriginalJob();
    }

    static TableResult runOriginalJob() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // Avoid unbounded state retention (60 seconds is enough for this example)
        tableEnv.getConfig().set("table.exec.state.ttl", "60000");

        tableEnv.executeSql(transactionsDDL);
        tableEnv.executeSql(printSinkDDL);

        return tableEnv.executeSql(streamingDeduplication);
    }

    public static void compileAndWritePlan(Path planLocation) {
        // Compiling plans is only possible in streaming mode
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // Overwrite an existing plan file, if any (useful for testing/debugging)
        tableEnv.getConfig().set("table.plan.force-recompile", "true");

        tableEnv.executeSql(transactionsDDL);
        tableEnv.executeSql(printSinkDDL);

        // Only queries that describe a complete pipeline (including an INSERT) can be compiled
        tableEnv.compilePlanSql(streamingDeduplication).writeToFile(planLocation);
    }

    public static TableResult runCompiledPlan(Path planLocation) {
        // Compiled plans can only be executed in streaming mode
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // Avoid unbounded state retention (60 seconds is enough for this example)
        tableEnv.getConfig().set("table.exec.state.ttl", "60000");

        return tableEnv.executePlan(PlanReference.fromFile(planLocation));
    }
}
