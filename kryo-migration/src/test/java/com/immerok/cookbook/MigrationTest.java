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

import static org.assertj.core.api.Assertions.assertThat;

import com.immerok.cookbook.extensions.FlinkMiniClusterExtension;
import com.immerok.cookbook.records.Event;
import com.immerok.cookbook.records.SubEventListTypeInfoFactory;
import java.nio.file.Path;
import java.util.List;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * The {@link #prepareSchemaEvolution()} and {@link #applySchemaEvolution()} methods provide a
 * 2-step procedure that allows you to experiment with schema evolution after migrating away from
 * Kryo.
 *
 * <ul>
 *   <li>1) Set {@link #MANUAL_SAVEPOINT_DIRECTORY} to some local directory.
 *   <li>2) Run {@link #prepareSchemaEvolution()}.
 *   <li>3) Make some changes to the {@link com.immerok.cookbook.records.SubEvent} class, like
 *       removing one of the {@code contentX} fields.
 *   <li>4) Run {@link #applySchemaEvolution()}.
 * </ul>
 *
 * <p>To see what would happen without migration, follow the above steps but instead of step 2) run
 * {@link #createSavepointWithKryo()} and set {@link #MANUAL_MIGRATED_SAVEPOINT} to the printed
 * savepoint path.
 */
@ExtendWith(FlinkMiniClusterExtension.class)
class MigrationTest {

    private static final String MANUAL_SAVEPOINT_DIRECTORY = "<insert path here>";
    private static final String MANUAL_MIGRATED_SAVEPOINT =
            MANUAL_SAVEPOINT_DIRECTORY + "/savepoint-migrated";

    @Test
    @Disabled("This is part of a manual procedure; see the class javadocs.")
    void prepareSchemaEvolution() throws Exception {
        final String savepoint = createSavepointWithKryo(MANUAL_SAVEPOINT_DIRECTORY);

        Migration.migrateSavepoint(savepoint, MANUAL_MIGRATED_SAVEPOINT);
    }

    @Test
    @Disabled("This is part of a manual procedure; see the class javadocs.")
    void applySchemaEvolution() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Migration.readSavepointAndExtractValueState(env, MANUAL_MIGRATED_SAVEPOINT).print();
        env.execute();
    }

    @Test
    @Disabled("This is part of a manual procedure; see the class javadocs.")
    void createSavepointWithKryo() throws Exception {
        System.out.println(createSavepointWithKryo(MANUAL_SAVEPOINT_DIRECTORY));
    }

    @Test
    void testMigration(@TempDir Path tempDir) throws Exception {
        final String savepoint = createSavepointWithKryo(tempDir.toAbsolutePath().toString());

        final Path migratedSavepointPath = tempDir.resolve("savepoint-migrated").toAbsolutePath();

        Migration.migrateSavepoint(savepoint, migratedSavepointPath.toString());

        final List<Event> state =
                Migration.readSavepointAndExtractValueState(
                                StreamExecutionEnvironment.getExecutionEnvironment(),
                                migratedSavepointPath.toString())
                        .executeAndCollect(100);
        assertThat(state).isNotEmpty();
    }

    private static String createSavepointWithKryo(final String savepointDirectory)
            throws Exception {
        try (AutoCloseable ignored = SubEventListTypeInfoFactory.temporarilyEnableKryoPath()) {

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            Job.defineWorkflow(env);

            final JobClient jobClient = env.executeAsync();

            // wait a bit for the job to have processed some data
            while (jobClient.getJobStatus().get() != JobStatus.RUNNING) {
                Thread.sleep(100);
            }
            Thread.sleep(5_000);

            return jobClient
                    .stopWithSavepoint(false, savepointDirectory, SavepointFormatType.CANONICAL)
                    .get();
        }
    }
}
