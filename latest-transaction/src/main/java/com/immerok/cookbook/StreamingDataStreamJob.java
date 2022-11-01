package com.immerok.cookbook;

import com.immerok.cookbook.records.Transaction;
import com.immerok.cookbook.records.TransactionDeserializer;
import com.immerok.cookbook.workflows.DataStreamWorkflow;
import java.util.function.Consumer;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;

public class StreamingDataStreamJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        setupJob(env, "transactions", workflow -> workflow.sinkTo(new PrintSink<>()));

        env.execute();
    }

    static void setupJob(
            StreamExecutionEnvironment env,
            String kafkaTopic,
            Consumer<DataStream<Transaction>> sinkApplier) {

        KafkaSource<Transaction> unboundedSource =
                KafkaSource.<Transaction>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics(kafkaTopic)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new TransactionDeserializer())
                        .build();

        DataStreamWorkflow.defineWorkflow(env, unboundedSource, sinkApplier);
    }
}
