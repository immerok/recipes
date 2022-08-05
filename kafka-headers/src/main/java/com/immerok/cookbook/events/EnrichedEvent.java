package com.immerok.cookbook.events;

public class EnrichedEvent {
    public Event event;
    public Metadata metadata;
    public Headers headers;

    /** A Flink POJO must have a no-args default constructor */
    public EnrichedEvent() {}

    public EnrichedEvent(Event event, Metadata metadata, Headers headers) {
        this.event = event;
        this.metadata = metadata;
        this.headers = headers;
    }

    @Override
    public String toString() {
        return String.format("EnrichedEvent{%n\t%s,%n\t%s,%n\t%s}", event, metadata, headers);
    }
}
