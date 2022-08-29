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
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;

/**
 * A {@link TypeInfoFactory} that allows us to control whether the {@code List<SubEvent>} is
 * serialized with Kryo or Flink's {@link org.apache.flink.api.common.typeutils.base.ListSerializer}
 * by annotating the field in the {@link Event} with {@link
 * org.apache.flink.api.common.typeinfo.TypeInfo}.
 *
 * <p>This factory allows us to read the {@code List<SubEvent>} with Kryo but write it with the list
 * serializer during migration, without having to change the POJO/job code.
 *
 * <p>Once the migration is complete the factory (and annotation!) should be kept around, with the
 * Kryo code path being removed.
 */
public class SubEventListTypeInfoFactory extends TypeInfoFactory<List<SubEvent>> {

    // this flag can (and should!) be removed after migration
    private static boolean USE_LIST_TYPE_INFO = true;

    private final boolean useListTypeInfo;

    public SubEventListTypeInfoFactory() {
        this.useListTypeInfo = USE_LIST_TYPE_INFO;
    }

    @Override
    public TypeInformation<List<SubEvent>> createTypeInfo(
            Type t, Map<String, TypeInformation<?>> genericParameters) {
        if (useListTypeInfo) {
            return new ListTypeInfo<>(TypeInformation.of(SubEvent.class));
        } else {
            // fall back to standard type extraction (i.e., use Kryo)
            return TypeInformation.of(new TypeHint<>() {});
        }
    }

    public static AutoCloseable temporarilyEnableKryoPath() {
        USE_LIST_TYPE_INFO = false;
        return () -> USE_LIST_TYPE_INFO = true;
    }
}
