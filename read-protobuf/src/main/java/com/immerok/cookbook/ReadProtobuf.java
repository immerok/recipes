package com.immerok.cookbook;

import java.util.function.Consumer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class ReadProtobuf {

    static final String TRANSACTION_TOPIC = "transactions";

    public static void main(String[] args) throws Exception {
        runJob();
    }

    static void runJob() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final TableDescriptor transactionStream =
                TableDescriptor.forConnector(KafkaDynamicTableFactory.IDENTIFIER)
                        .schema(
                                Schema.newBuilder()
                                        .column("t_time", DataTypes.STRING())
                                        .column("t_id", DataTypes.BIGINT())
                                        .column("t_customer_id", DataTypes.BIGINT())
                                        .column("t_amount", DataTypes.DOUBLE())
                                        .build())
                        .format("protobuf")
                        .option("protobuf.message-class-name", Transaction.class.getName())
                        .option("topic", TRANSACTION_TOPIC)
                        .option("properties.bootstrap.servers", "localhost:9092")
                        .option("properties.group.id", "ReadProtobuf-Job")
                        .option("scan.startup.mode", "earliest-offset")
                        .build();

        defineWorkflow(tableEnv, transactionStream, workflow -> workflow.sinkTo(new PrintSink<>()));

        env.execute();
    }

    static void defineWorkflow(
            StreamTableEnvironment tableEnv,
            TableDescriptor transactionStream,
            Consumer<DataStream<Row>> sinkApplier) {

        tableEnv.createTemporaryTable("KafkaSourceWithProtobufEncodedEvents", transactionStream);

        Table resultTable =
                tableEnv.sqlQuery(
                        String.join("\n", "SELECT * FROM KafkaSourceWithProtobufEncodedEvents"));

        // switch back from Table API to DataStream
        // when a regular select statement runs in streaming mode it produces an insert-only
        // changelog stream
        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);

        // attach the sink
        sinkApplier.accept(resultStream);
    }
}
