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
package com.immerok.cookbook.records;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.junit.jupiter.api.Test;

class SubEventListTypeInfoFactoryTest {

    /**
     * Verify that the {@link SubEventListTypeInfoFactory} by default prevents Kryo to be used for
     * serialization. This ensures the newly written savepoint will be free of Kryo.
     */
    @Test
    void testDoesNotRequireKryoByDefault() {
        final ExecutionConfig executionConfig = new ExecutionConfig();
        // disable generic types to force a failure if Kryo is actually used
        executionConfig.disableGenericTypes();

        TypeSerializer<Event> eventSerializer =
                TypeInformation.of(Event.class).createSerializer(new ExecutionConfig());

        assertThat(eventSerializer).isInstanceOf(PojoSerializer.class);
    }

    /**
     * Verify that {@link SubEventListTypeInfoFactory#temporarilyEnableKryoPath()} results in Kryo
     * being used for serialization. This ensures that we are able to read the original savepoint.
     */
    @Test
    void testTemporarilyEnableKryoPath() throws Exception {
        try (AutoCloseable ignored = SubEventListTypeInfoFactory.temporarilyEnableKryoPath()) {
            final ExecutionConfig executionConfig = new ExecutionConfig();
            // disable generic types to force a failure if Kryo is actually used
            executionConfig.disableGenericTypes();

            assertThatThrownBy(
                            () -> TypeInformation.of(Event.class).createSerializer(executionConfig))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }
}