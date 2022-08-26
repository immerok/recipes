package com.immerok.cookbook.records;

import static com.immerok.cookbook.records.SensorReading.HOT;

import java.time.Instant;
import java.util.function.Supplier;

/** A supplier that produces SensorReadings that oscillate between hot and cool. */
public class OscillatingSensorReadingSupplier implements Supplier<SensorReading> {
    private final long secondsOfHeat;

    /**
     * @param secondsOfHeat The sensors run HOT for secondsOfHeat seconds and then are cool for
     *     secondsOfHeat seconds. Ideally, secondsOfHeat should be a divisor of 60.
     */
    public OscillatingSensorReadingSupplier(long secondsOfHeat) {
        this.secondsOfHeat = secondsOfHeat;
    }

    @Override
    public SensorReading get() {
        final long deviceId = 0;
        final Instant now = Instant.now();
        final long secondsInTheCurrentMinute = now.getEpochSecond() % 60;
        final long temp =
                ((secondsInTheCurrentMinute % (2 * secondsOfHeat)) < secondsOfHeat)
                        ? HOT + 10L
                        : HOT - 10L;

        return new SensorReading(deviceId, temp, now);
    }
}
