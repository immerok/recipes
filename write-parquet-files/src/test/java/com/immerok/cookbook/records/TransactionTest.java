package com.immerok.cookbook.records;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.junit.jupiter.api.Test;

class TransactionTest {
    /**
     * Verify that Flink recognizes the Transaction type as a POJO that it can serialize
     * efficiently.
     */
    @Test
    void testRecognizedAsPojo() {
        TypeSerializer<Transaction> eventSerializer =
                TypeInformation.of(Transaction.class).createSerializer(new ExecutionConfig());

        assertThat(eventSerializer).isInstanceOf(PojoSerializer.class);
    }
}
