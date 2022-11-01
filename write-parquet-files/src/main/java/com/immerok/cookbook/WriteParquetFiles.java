package com.immerok.cookbook;

import com.immerok.cookbook.records.Transaction;
import com.immerok.cookbook.records.TransactionDeserializer;
import java.time.Duration;
import java.time.ZoneId;
import java.util.function.Consumer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class WriteParquetFiles {

    static final String TRANSACTION_TOPIC = "transactions";

    public static void main(String[] args) {
        final ParameterTool parameters = ParameterTool.fromArgs(args);

        Path outputFolder = new Path(parameters.getRequired("outputFolder"));

        runJob(outputFolder);
    }

    static void runJob(Path dataDirectory) {
        KafkaSource<Transaction> transactionSource =
                KafkaSource.<Transaction>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics(TRANSACTION_TOPIC)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new TransactionDeserializer())
                        .build();
        runJob(
                dataDirectory,
                transactionSource,
                ignored -> {},
                EnvironmentSettings.inStreamingMode());
    }

    static void runJob(
            Path dataDirectory,
            KafkaSource<Transaction> transactionSource,
            Consumer<TableResult> insertResultConsumer,
            EnvironmentSettings environmentSettings) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);

        DataStream<Transaction> transactionStream =
                env.fromSource(transactionSource, WatermarkStrategy.noWatermarks(), "Transactions");

        tableEnv.getConfig()
                .set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10))
                .setLocalTimeZone(ZoneId.of("UTC"));

        // seamlessly switch from DataStream to Table API
        tableEnv.createTemporaryView("Transactions", transactionStream);

        tableEnv.createTable(
                "ParquetSink",
                createFilesystemConnectorDescriptor(dataDirectory)
                        .partitionedBy("t_date")
                        .option("auto-compaction", "true")
                        .build());

        TableResult tableResult =
                tableEnv.executeSql(
                        "INSERT INTO ParquetSink "
                                // Flink maps timestamps to INT96,
                                // for accurate results we cast the timestamp to a string
                                + "  SELECT CAST(t_time AS STRING), "
                                + "  DATE_FORMAT(`t_time`,'yyyy_MM_dd'), "
                                + "  t_id, "
                                + "  t_customer_id, "
                                + "  t_amount "
                                + "  FROM Transactions");

        insertResultConsumer.accept(tableResult);
    }

    static TableDescriptor.Builder createFilesystemConnectorDescriptor(Path dataDirectory) {
        return TableDescriptor.forConnector("filesystem")
                .schema(
                        Schema.newBuilder()
                                .column("t_time", DataTypes.STRING())
                                .column("t_date", DataTypes.STRING())
                                .column("t_id", DataTypes.BIGINT())
                                .column("t_customer_id", DataTypes.BIGINT())
                                .column("t_amount", DataTypes.DECIMAL(10, 2))
                                .build())
                .option("path", dataDirectory.toString())
                .option("format", "parquet");
    }
}
