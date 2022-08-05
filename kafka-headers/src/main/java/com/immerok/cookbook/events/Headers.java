package com.immerok.cookbook.events;

import java.util.Objects;

/**
 * A collection of custom created Headers sent with a Kafka record that Flink recognizes as a valid
 * POJO
 */
public class Headers {

    /** A Flink POJO must have public fields, or getters and setters */
    public String tracestate;

    public String traceparent;

    /** A Flink POJO must have a no-args default constructor */
    public Headers() {}

    public Headers(final String tracestate, final String traceparent) {
        this.tracestate = tracestate;
        this.traceparent = traceparent;
    }

    /** Used for printing during development */
    @Override
    public String toString() {
        return "Headers{" + "tracestate=" + tracestate + ", traceparent='" + traceparent + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Headers headers = (Headers) o;
        return traceparent.equals(headers.tracestate) && traceparent.equals(headers.traceparent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(traceparent, traceparent);
    }
}
