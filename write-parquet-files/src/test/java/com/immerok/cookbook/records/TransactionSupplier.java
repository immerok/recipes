package com.immerok.cookbook.records;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Random;
import java.util.function.Supplier;

/** A supplier that produces duplicated Transactions. */
public class TransactionSupplier implements Supplier<Transaction> {
    private static final int TOTAL_CUSTOMERS = 25;

    private static final Random random = new Random();
    private int id = 0;

    @Override
    public Transaction get() {
        return new Transaction(
                Instant.now(),
                this.id++,
                random.nextInt(TOTAL_CUSTOMERS),
                new BigDecimal(1000.0 * random.nextFloat()));
    }
}
