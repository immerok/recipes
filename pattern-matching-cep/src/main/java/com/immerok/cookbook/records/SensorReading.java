package com.immerok.cookbook.records;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.Instant;
import java.util.Objects;

/**
 * SensorReading is a class that Flink recognizes as a valid POJO.
 *
 * <p>Because it has valid (deterministic) implementations of equals and hashcode, this type can
 * also be used a key.
 */
public class SensorReading {

    public static long HOT = 50L;

    // A Flink POJO must have public fields, or getters and setters
    public long deviceId;
    public long temperature;

    // Serialize the timestamps in ISO-8601 format for greater compatibility
    @JsonFormat(
            shape = JsonFormat.Shape.STRING,
            pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            timezone = "UTC")
    public Instant timestamp;

    // A Flink POJO must have a no-args default constructor
    public SensorReading() {}

    public SensorReading(final long deviceId, final long temperature, final Instant timestamp) {
        this.deviceId = deviceId;
        this.temperature = temperature;
        this.timestamp = timestamp;
    }

    public boolean sensorIsHot() {
        return temperature >= HOT;
    }

    public boolean sensorIsCool() {
        return temperature < HOT;
    }

    // Used for printing during development
    @Override
    public String toString() {
        return "Event{"
                + "deviceId="
                + deviceId
                + ", temperature='"
                + temperature
                + '\''
                + ", timestamp="
                + timestamp
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SensorReading sensorReading = (SensorReading) o;

        return (deviceId == sensorReading.deviceId)
                && (temperature == sensorReading.temperature)
                && timestamp.equals(sensorReading.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deviceId, temperature, timestamp);
    }
}
