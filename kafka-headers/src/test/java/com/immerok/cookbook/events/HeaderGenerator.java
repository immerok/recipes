/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.immerok.cookbook.events;

import static com.immerok.cookbook.events.KafkaHeadersEventDeserializationSchema.HEADER_TRACE_PARENT;
import static com.immerok.cookbook.events.KafkaHeadersEventDeserializationSchema.HEADER_TRACE_STATE;
import static org.apache.commons.text.CharacterPredicates.DIGITS;
import static org.apache.commons.text.CharacterPredicates.LETTERS;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.function.Function;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.kafka.common.header.internals.RecordHeaders;

/** A generator for Kafka record headers. */
public class HeaderGenerator implements Function<Event, org.apache.kafka.common.header.Headers> {

    private final RandomStringGenerator generator =
            new RandomStringGenerator.Builder()
                    .withinRange('0', 'f')
                    .filteredBy(LETTERS, DIGITS)
                    .build();

    @Override
    public org.apache.kafka.common.header.Headers apply(Event event) {
        return new RecordHeaders()
                .add(HEADER_TRACE_STATE, asBytes("apache_flink=" + generate(11)))
                .add(
                        HEADER_TRACE_PARENT,
                        asBytes("00-" + generate(32) + "-" + generate(16) + "-01"));
    }

    private String generate(int length) {
        return generator.generate(length).toLowerCase(Locale.ROOT);
    }

    private static byte[] asBytes(String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }
}
