package com.immerok.cookbook.records;

import org.apache.flink.types.PojoTestUtils;
import org.junit.jupiter.api.Test;

class TransactionTest {
    /**
     * Verify that Flink recognizes the Transaction type as a POJO that it can serialize
     * efficiently.
     */
    @Test
    void testRecognizedAsPojo() {
        PojoTestUtils.assertSerializedAsPojo(Transaction.class);
    }
}
