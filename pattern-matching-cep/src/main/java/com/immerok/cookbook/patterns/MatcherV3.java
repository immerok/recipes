package com.immerok.cookbook.patterns;

import com.immerok.cookbook.conditions.StillHotLater;
import com.immerok.cookbook.records.SensorReading;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.util.Collector;

public class MatcherV3 implements PatternMatcher<SensorReading, SensorReading> {

    public Pattern<SensorReading, ?> pattern(Duration limitOfHeatTolerance) {
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();

        return Pattern.<SensorReading>begin("starts-cool", skipStrategy)
                .where(
                        new SimpleCondition<>() {
                            @Override
                            public boolean filter(SensorReading reading) {
                                return reading.sensorIsCool();
                            }
                        })
                .next("gets-hot")
                .where(
                        new SimpleCondition<>() {
                            @Override
                            public boolean filter(SensorReading reading) {
                                return reading.sensorIsHot();
                            }
                        })
                .oneOrMore()
                .consecutive()
                .next("stays-hot-long-enough")
                .where(new StillHotLater("gets-hot", limitOfHeatTolerance));
    }

    public PatternProcessFunction<SensorReading, SensorReading> process() {

        return new PatternProcessFunction<>() {
            @Override
            public void processMatch(
                    Map<String, List<SensorReading>> map,
                    Context context,
                    Collector<SensorReading> out) {
                SensorReading event = map.get("stays-hot-long-enough").get(0);
                out.collect(event);
            }
        };
    }
}
