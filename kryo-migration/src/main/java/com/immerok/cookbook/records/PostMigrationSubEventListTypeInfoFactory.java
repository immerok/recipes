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

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;

/**
 * A {@link TypeInfoFactory} that allows serialization of {@code List<SubEvent>} with Flink's {@link
 * org.apache.flink.api.common.typeutils.base.ListSerializer}. This prevents the usage of Kryo, and
 * thus allows schema evolution of the {@link SubEvent}.
 *
 * <p>Usage:Annotate the field in the {@link Event} with {@link
 * org.apache.flink.api.common.typeinfo.TypeInfo} and pass this factory.
 */
@SuppressWarnings("unused")
public class PostMigrationSubEventListTypeInfoFactory extends TypeInfoFactory<List<SubEvent>> {

    @Override
    public TypeInformation<List<SubEvent>> createTypeInfo(
            Type t, Map<String, TypeInformation<?>> genericParameters) {
        return new ListTypeInfo<>(TypeInformation.of(SubEvent.class));
    }
}
