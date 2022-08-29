package com.immerok.cookbook.records;

/**
 * The POJO for which schema evolution is blocked by Kryo because it's contained in a list within
 * {@link Event}.
 */
public class SubEvent {

    /** A Flink POJO must have public fields, or getters and setters */
    public String content1;

    public String content2;

    /** A Flink POJO must have a no-args default constructor */
    public SubEvent() {}

    public SubEvent(String content1, String content2) {
        this.content1 = content1;
        this.content2 = content2;
    }

    @Override
    public String toString() {
        return "SubEvent{"
                + "content1='"
                + content1
                + '\''
                + ", content2='"
                + content2
                + '\''
                + '}';
    }
}
