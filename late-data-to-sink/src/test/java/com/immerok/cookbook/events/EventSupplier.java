package com.immerok.cookbook.events;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.function.Supplier;
import net.datafaker.Faker;

/** A supplier that produces Events. */
public class EventSupplier implements Supplier<Event> {
    private final Faker faker = new Faker();
    private final String lateElementData;

    private int id = 1;

    public EventSupplier(String lateElementData) {
        this.lateElementData = lateElementData;
    }

    @Override
    public Event get() {
        String lotrCharacter = faker.lordOfTheRings().character();

        // every 10th element is 2 seconds late
        // we are emitting late elements with a delay of 50 elements, because there is a period on
        // job start where no element is considered late (see FLINK-28538)
        if (id > 50 && id % 10 == 0) {
            return new Event(id++, lateElementData, Instant.now().minus(5, ChronoUnit.HOURS));
        }

        return new Event(id++, lotrCharacter, Instant.now());
    }
}
