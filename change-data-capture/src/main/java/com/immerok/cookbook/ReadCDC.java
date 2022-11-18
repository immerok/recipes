package com.immerok.cookbook;

import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import java.util.function.Consumer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class ReadCDC {

    public static void main(String[] args) throws Exception {
        runJob();
    }

    static void runJob() throws Exception {
        SourceFunction<String> source =
                PostgreSQLSource.<String>builder()
                        .hostname("localhost")
                        .port(5432)
                        .database("postgres")
                        .schemaList("transactions")
                        .tableList("transactions.incoming")
                        .username("postgres")
                        .password("postgres")
                        // Set the logical decoding plugin to pgoutput which
                        // is used by the embedded postgres database
                        .decodingPluginName("pgoutput")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        defineWorkflow(env, source, workflow -> workflow.sinkTo(new PrintSink<>()));
        env.execute();
    }

    static void defineWorkflow(
            StreamExecutionEnvironment env,
            SourceFunction<String> source,
            Consumer<DataStream<String>> sinkApplier) {
        final DataStreamSource<String> postgres = env.addSource(source).setParallelism(1);

        // additional workflow steps go here
        sinkApplier.accept(postgres);
    }
}
