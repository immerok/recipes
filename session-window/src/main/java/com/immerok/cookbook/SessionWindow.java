package com.immerok.cookbook;

import com.immerok.cookbook.events.Event;
import com.immerok.cookbook.events.EventDeserializationSchema;
import com.immerok.cookbook.events.UserActivity;
import java.util.function.Consumer;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SessionWindow {

    static final String TOPIC = "input";

    public static void main(String[] args) throws Exception {
        runJob();
    }

    static void runJob() throws Exception {
        KafkaSource<Event> source =
                KafkaSource.<Event>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics(TOPIC)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new EventDeserializationSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        defineWorkflow(env, source, workflow -> workflow.sinkTo(new PrintSink<>()));
        env.execute();
    }

    static void defineWorkflow(
            StreamExecutionEnvironment env,
            Source<Event, ?, ?> source,
            Consumer<DataStream<UserActivity>> sinkApplier) {
        final DataStreamSource<Event> kafka =
                env.fromSource(
                                source,
                                WatermarkStrategy.<Event>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (element, recordTimestamp) ->
                                                        element.timestamp.toEpochMilli()),
                                "Kafka")
                        .setParallelism(1);

        final SingleOutputStreamOperator<UserActivity> userActivities =
                kafka.keyBy(event -> event.user)
                        .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                        .aggregate(
                                new AggregateFunction<Event, UserActivity, UserActivity>() {
                                    @Override
                                    public UserActivity createAccumulator() {
                                        return new UserActivity();
                                    }

                                    @Override
                                    public UserActivity add(Event value, UserActivity accumulator) {
                                        accumulator.user = value.user;
                                        accumulator.activityStart =
                                                ObjectUtils.min(
                                                        value.timestamp, accumulator.activityStart);
                                        accumulator.activityEnd =
                                                ObjectUtils.max(
                                                        value.timestamp, accumulator.activityEnd);
                                        accumulator.numInteractions += 1;
                                        return accumulator;
                                    }

                                    @Override
                                    public UserActivity merge(UserActivity a, UserActivity b) {
                                        a.numInteractions += b.numInteractions;
                                        a.activityStart =
                                                ObjectUtils.min(a.activityStart, b.activityStart);
                                        a.activityEnd =
                                                ObjectUtils.max(a.activityEnd, b.activityEnd);
                                        return a;
                                    }

                                    @Override
                                    public UserActivity getResult(UserActivity accumulator) {
                                        return accumulator;
                                    }
                                });

        // additional workflow steps go here

        sinkApplier.accept(userActivities);
    }
}
