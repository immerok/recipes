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

package com.immerok.cookbook;

import com.immerok.cookbook.functions.LatestEventFunction;
import com.immerok.cookbook.records.Event;
import com.immerok.cookbook.records.SubEventListTypeInfoFactory;
import com.immerok.cookbook.utils.SimpleValueStateBootstrapFunction;
import com.immerok.cookbook.utils.SimpleValueStateReaderFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.state.api.SavepointWriter;
import org.apache.flink.state.api.StateBootstrapTransformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Migrates the value state of the {@link LatestEventFunction} away from Kryo. */
public class Migration {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        final String sourceSavepoint = parameterTool.getRequired("sourceSavepoint");
        final String targetSavepoint = parameterTool.getRequired("targetSavepoint");

        migrateSavepoint(sourceSavepoint, targetSavepoint);
    }

    static void migrateSavepoint(final String sourceSavepointPath, final String targetSavepointPath)
            throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // extract the state while using the Kryo serializer
        final DataStream<Event> state;
        try (AutoCloseable ignored = SubEventListTypeInfoFactory.temporarilyEnableKryoPath()) {
            state = readSavepointAndExtractValueState(env, sourceSavepointPath);
        }

        // write a new savepoint while using the List serializer (new default set by the
        // SubEventListTypeInfoFactory)
        writeSavepointWithValueState(state, sourceSavepointPath, targetSavepointPath);

        env.execute();
    }

    static DataStream<Event> readSavepointAndExtractValueState(
            final StreamExecutionEnvironment env, final String savepointPath) throws Exception {
        final SavepointReader savepoint = SavepointReader.read(env, savepointPath);

        return savepoint.readKeyedState(
                Job.STATEFUL_OPERATOR_UID,
                new SimpleValueStateReaderFunction<>(LatestEventFunction.createStateDescriptor()),
                TypeInformation.of(Long.class),
                TypeInformation.of(Event.class));
    }

    static void writeSavepointWithValueState(
            final DataStream<Event> state,
            final String sourceSavepointPath,
            final String targetSavepointPath)
            throws Exception {
        final SavepointWriter savepointWriter =
                SavepointWriter.fromExistingSavepoint(sourceSavepointPath);

        final StateBootstrapTransformation<Event> transformation =
                OperatorTransformation.bootstrapWith(state)
                        .keyBy(Job.getEventKeySelector())
                        .transform(
                                new SimpleValueStateBootstrapFunction<>(
                                        LatestEventFunction.createStateDescriptor()));

        savepointWriter
                .removeOperator(Job.STATEFUL_OPERATOR_UID)
                .withOperator(Job.STATEFUL_OPERATOR_UID, transformation);

        savepointWriter.write(targetSavepointPath);
    }
}
