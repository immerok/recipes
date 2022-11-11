package com.immerok.cookbook.events;

import java.time.Instant;
import java.util.Objects;

/** A simple Event class that Flink recognizes as a valid POJO. */
public class Event {

    public long id;
    public String name;
    public Instant timestamp;

    public Event() {}

    public Event(final long id, final String name, final Instant timestamp) {
        this.id = id;
        this.name = name;
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Event)) {
            return false;
        }
        Event event = (Event) o;
        return id == event.id
                && Objects.equals(name, event.name)
                && Objects.equals(timestamp, event.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, timestamp);
    }

    @Override
    public String toString() {
        return "Event{" + "id=" + id + ", name='" + name + '\'' + ", timestamp=" + timestamp + '}';
    }
}
