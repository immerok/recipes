package com.immerok.cookbook.events;

import java.time.Instant;
import java.util.Objects;

/**
 * A simple Event that Flink recognizes as a valid POJO.
 *
 * <p>Because it has valid (deterministic) implementations of equals and hashcode, this type can
 * also be used a key.
 */
public class Event {

    /** A Flink POJO must have public fields, or getters and setters */
    public long id;

    public String data;
    public Instant timestamp;

    /** A Flink POJO must have a no-args default constructor */
    public Event() {}

    public Event(final long id, final String data, final Instant timestamp) {
        this.id = id;
        this.data = data;
        this.timestamp = timestamp;
    }

    /** Used for printing during development */
    @Override
    public String toString() {
        return "Event{" + "id=" + id + ", data='" + data + '\'' + ", timestamp=" + timestamp + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Event event = (Event) o;
        return id == event.id && data.equals(event.data) && timestamp.equals(event.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, data, timestamp);
    }
}
