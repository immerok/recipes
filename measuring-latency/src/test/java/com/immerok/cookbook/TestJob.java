package com.immerok.cookbook;

import com.immerok.cookbook.events.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TestJob {
    public static final int ROWS_PER_SECOND = 1000;

    private static final TableDescriptor SOURCE_DESCRIPTOR =
            TableDescriptor.forConnector("datagen")
                    .schema(
                            Schema.newBuilder()
                                    .column("id", DataTypes.BIGINT())
                                    .column("name", DataTypes.STRING())
                                    .column("timestamp", DataTypes.TIMESTAMP_LTZ(3))
                                    .build())
                    .option("rows-per-second", String.valueOf(ROWS_PER_SECOND))
                    .option("fields.id.min", "100")
                    .option("fields.id.max", "999")
                    .option("fields.name.length", "12")
                    .option("fields.timestamp.max-past", "0")
                    .build();

    private static DataStream<Event> createUnboundedTestStream(StreamExecutionEnvironment env) {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        final Table sourceTable = tableEnv.from(SOURCE_DESCRIPTOR);

        return tableEnv.toDataStream(sourceTable, Event.class);
    }

    public static void runUnbounded() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MeasuringLatency.defineWorkflow(
                TestJob.createUnboundedTestStream(env),
                workflow -> workflow.addSink(new DiscardingSink<>()),
                10 * ROWS_PER_SECOND);

        env.execute();
    }
}
