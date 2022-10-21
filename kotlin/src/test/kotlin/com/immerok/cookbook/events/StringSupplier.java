package com.immerok.cookbook.events;

import java.util.function.Supplier;
import net.datafaker.Faker;

/** A supplier produces Strings. */
public class StringSupplier implements Supplier<String> {
    private final Faker faker = new Faker();

    @Override
    public String get() {
        return faker.lordOfTheRings().character();
    }
}
