package com.immerok.cookbook.records;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Random;
import java.util.function.Supplier;

/** An supplier that produces duplicated Transactions. */
public class DuplicatingTransactionSupplier implements Supplier<Transaction> {
    public static final int TOTAL_CUSTOMERS = 6;
    private static final Random random = new Random();
    private int id = 0;
    private Transaction lastTransaction;

    @Override
    public Transaction get() {
        if (id++ % 2 == 0) {
            lastTransaction = transactionForEvenID();
        }

        return lastTransaction;
    }

    private Transaction transactionForEvenID() {
        this.lastTransaction =
                new Transaction(
                        Instant.now(),
                        this.id,
                        random.nextInt(TOTAL_CUSTOMERS),
                        new BigDecimal(1000.0 * random.nextFloat()));

        return lastTransaction;
    }
}
