package com.immerok.cookbook;

import com.immerok.cookbook.functions.LatestEventFunction;
import com.immerok.cookbook.records.Event;
import com.immerok.cookbook.records.SubEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * A Flink job that contains a keyed value-state that is originally partially serialized with Kryo.
 */
public class Job {

    static final String STATEFUL_OPERATOR_UID = "stateful-operator";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        defineWorkflow(env);
        env.execute();
    }

    static void defineWorkflow(StreamExecutionEnvironment env) {
        DataStream<Event> transactionStream =
                env.fromSequence(0, 1_000_000_000).map(new EventGenerator());

        transactionStream
                .keyBy(getEventKeySelector())
                .process(new LatestEventFunction())
                .uid(STATEFUL_OPERATOR_UID);
    }

    public static KeySelector<Event, Long> getEventKeySelector() {
        return event -> event.userId;
    }

    private static class EventGenerator implements MapFunction<Long, Event> {
        private static final int TOTAL_CUSTOMERS = 2;

        private static final Random random = new Random();

        @Override
        public Event map(Long value) {
            return new Event(
                    random.nextInt(TOTAL_CUSTOMERS),
                    new ArrayList<>(
                            List.of(
                                    new SubEvent(
                                            String.valueOf(random.nextInt()),
                                            String.valueOf(TOTAL_CUSTOMERS)))));
        }
    }
}
