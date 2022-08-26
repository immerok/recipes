package com.immerok.cookbook.patterns;

import java.time.Duration;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;

public interface PatternMatcher<IN, OUT> {

    Pattern<IN, ?> pattern(Duration limitOfHeatTolerance);

    PatternProcessFunction<IN, OUT> process();
}
