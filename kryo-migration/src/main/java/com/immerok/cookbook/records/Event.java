package com.immerok.cookbook.records;

import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInfo;

/** The top-level POJO that is stored in state. */
public class Event {

    /** A Flink POJO must have public fields, or getters and setters */
    public long userId;

    /**
     * This list (and its contents) are by default serialized by Kryo since Flink does not use the
     * built-in List serializer by default.
     *
     * <p>Note: Usually the {@link TypeInfo} annotation would not be present while the savepoint is
     * taken, instead being added after the savepoint is created and before the migration begins.
     */
    @TypeInfo(SubEventListTypeInfoFactory.class)
    public List<SubEvent> subEvents;

    /** A Flink POJO must have a no-args default constructor */
    public Event() {}

    public Event(long userId, List<SubEvent> subEvents) {
        this.userId = userId;
        this.subEvents = subEvents;
    }

    @Override
    public String toString() {
        return "Event{" + "userId=" + userId + ", annotations=" + subEvents + '}';
    }
}
