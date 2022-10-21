package com.immerok.cookbook.events;

import java.time.Instant;
import java.util.function.Supplier;
import net.datafaker.Faker;

/** A supplier of events. Every 10th element is malformed. */
public class EventSupplier implements Supplier<Object> {
    private final Faker faker = new Faker();
    private int id = 0;

    @Override
    public Object get() {
        String lotrCharacter = faker.lordOfTheRings().character();
        id++;
        if (id % 10 == 0) {
            return new MalformedEvent(lotrCharacter, Instant.now());
        }
        return new Event(id, lotrCharacter, Instant.now());
    }
}
