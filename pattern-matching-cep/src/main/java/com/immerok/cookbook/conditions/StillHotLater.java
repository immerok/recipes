package com.immerok.cookbook.conditions;

import com.immerok.cookbook.records.SensorReading;
import java.time.Duration;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

public class StillHotLater extends IterativeCondition<SensorReading> {

    private final String nameOfInitialHotPattern;
    private final Duration limitOfHeatTolerance;

    public StillHotLater(String nameOfInitialHotPattern, Duration limitOfHeatTolerance) {
        this.nameOfInitialHotPattern = nameOfInitialHotPattern;
        this.limitOfHeatTolerance = limitOfHeatTolerance;
    }

    @Override
    public boolean filter(SensorReading thisReading, Context<SensorReading> context)
            throws Exception {

        boolean sensorIsHotNow = thisReading.sensorIsHot();

        SensorReading firstHotReading =
                context.getEventsForPattern(nameOfInitialHotPattern).iterator().next();

        Duration interval = Duration.between(firstHotReading.timestamp, thisReading.timestamp);

        return sensorIsHotNow && interval.compareTo(limitOfHeatTolerance) >= 0;
    }
}
