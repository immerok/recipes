package com.immerok.cookbook.records;

import java.time.Instant;
import java.util.function.Supplier;

/** A supplier that produces SensorReadings with ever rising temperatures. */
public class RisingSensorReadingSupplier implements Supplier<SensorReading> {
    private long temp;

    public RisingSensorReadingSupplier(long initialTemp) {
        this.temp = initialTemp;
    }

    @Override
    public SensorReading get() {
        final long deviceId = 0;
        final Instant now = Instant.now();

        return new SensorReading(deviceId, temp++, now);
    }
}
