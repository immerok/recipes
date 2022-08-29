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

package com.immerok.cookbook.utils;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.util.Collector;

/**
 * A {@link KeyedStateReaderFunction} that returns the value contained in the {@link ValueState}.
 */
public class SimpleValueStateReaderFunction<K, T> extends KeyedStateReaderFunction<K, T> {

    private final ValueStateDescriptor<T> descriptor;
    private ValueState<T> state;

    public SimpleValueStateReaderFunction(ValueStateDescriptor<T> descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public void open(Configuration configuration) {
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void readKey(K k, Context context, Collector<T> collector) throws Exception {
        collector.collect(state.value());
    }
}
