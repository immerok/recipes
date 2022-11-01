package com.immerok.cookbook;

import com.immerok.cookbook.records.Customer;
import com.immerok.cookbook.records.CustomerDeserializer;
import com.immerok.cookbook.records.Transaction;
import com.immerok.cookbook.records.TransactionDeserializer;
import java.util.function.Consumer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableDeduplicatedJoin {

    static final String TRANSACTION_TOPIC = "transactions";
    static final String CUSTOMER_TOPIC = "customers";

    public static void main(String[] args) throws Exception {
        runJob();
    }

    static void runJob() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        KafkaSource<Customer> customerSource =
                KafkaSource.<Customer>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics(CUSTOMER_TOPIC)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new CustomerDeserializer())
                        .build();

        DataStream<Customer> customerStream =
                env.fromSource(customerSource, WatermarkStrategy.noWatermarks(), "Customers");

        KafkaSource<Transaction> transactionSource =
                KafkaSource.<Transaction>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics(TRANSACTION_TOPIC)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new TransactionDeserializer())
                        .build();

        DataStream<Transaction> transactionStream =
                env.fromSource(transactionSource, WatermarkStrategy.noWatermarks(), "Transactions");

        defineWorkflow(
                tableEnv,
                customerStream,
                transactionStream,
                workflow -> workflow.sinkTo(new PrintSink<>()));

        env.execute();
    }

    static void defineWorkflow(
            StreamTableEnvironment tableEnv,
            DataStream<Customer> customerStream,
            DataStream<Transaction> transactionStream,
            Consumer<DataStream<Row>> sinkApplier) {

        // seamlessly switch from DataStream to Table API
        tableEnv.createTemporaryView("Customers", customerStream);
        tableEnv.createTemporaryView("Transactions", transactionStream);

        // use Flink SQL to do the heavy lifting
        // this particular query can be dangerously expensive; see the recipe docs for details
        Table resultTable =
                tableEnv.sqlQuery(
                        String.join(
                                "\n",
                                "SELECT t_id, c_name, CAST(t_amount AS DECIMAL(5, 2))",
                                "FROM Customers",
                                "JOIN (SELECT DISTINCT * FROM Transactions) ON c_id = t_customer_id"));

        // switch back from Table API to DataStream
        // when a regular join runs in streaming mode it produces an updating changelog stream
        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);

        // attach the sink
        sinkApplier.accept(resultStream);
    }
}
