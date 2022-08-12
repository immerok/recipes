package com.immerok.cookbook;

import com.immerok.cookbook.workflows.TableWorkflow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class StreamingTableJob {

    public static void main(String[] args) {
        produceResults("transactions").print();
    }

    static TableResult produceResults(String kafkaTopic) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql(
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
                        kafkaTopic));

        return TableWorkflow.defineWorkflow(tableEnv, "Transactions");
    }
}
