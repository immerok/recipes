package com.immerok.cookbook.functions;

import com.immerok.cookbook.records.Event;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * A {@link KeyedProcessFunction} that stores the last received {@link Event} in a {@link
 * ValueState}.
 *
 * <p>This class isn't useful for any practical applications; its only purpose is to be a stateful
 * operation.
 */
public class LatestEventFunction extends KeyedProcessFunction<Long, Event, Event> {

    private ValueState<Event> latestEvent;

    @Override
    public void open(Configuration config) {
        latestEvent = getRuntimeContext().getState(createStateDescriptor());
    }

    @Override
    public void processElement(
            Event incoming,
            KeyedProcessFunction<Long, Event, Event>.Context context,
            Collector<Event> out)
            throws Exception {
        latestEvent.update(incoming);
    }

    public static ValueStateDescriptor<Event> createStateDescriptor() {
        return new ValueStateDescriptor<>("latest event", TypeInformation.of(Event.class));
    }
}
