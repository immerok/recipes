package com.immerok.cookbook;

import static org.assertj.core.api.Assertions.assertThat;

import com.immerok.cookbook.events.DataGenerator;
import com.immerok.cookbook.events.Event;
import com.immerok.cookbook.extensions.MiniClusterExtensionFactory;
import java.io.File;
import java.util.List;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class ContinuousFileReadingTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK =
            MiniClusterExtensionFactory.withDefaultConfiguration();

    /**
     * Runs the production job against continuously generated test files into temporarily {@code
     * targetDirectory}
     *
     * <p>This is a manual test because this job will never finish.
     */
    @Test
    @Disabled("Not running 'testProductionJob()' because it is a manual test.")
    void testProductionJob(@TempDir File tempDirectory, @TempDir File readDirectory)
            throws Exception {
        DataGenerator.generateFilesAsync(tempDirectory, readDirectory);
        ContinuousFileReading.runJob(Path.fromLocalFile(readDirectory));
    }

    @Test
    void JobProducesAtLeastOneResult(@TempDir File tempDirectory, @TempDir File readDirectory)
            throws Exception {
        final List<Event> expectedEvents = DataGenerator.generateFile(tempDirectory, readDirectory);

        final DataStream.Collector<Event> testSink = new DataStream.Collector<>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ContinuousFileReading.defineWorkflow(
                env,
                Path.fromLocalFile(readDirectory),
                workflow -> workflow.collectAsync(testSink));
        env.executeAsync();

        try (final CloseableIterator<Event> output = testSink.getOutput()) {
            for (Event expectedEvent : expectedEvents) {
                assertThat(output).hasNext();
                assertThat(output.next()).isEqualTo(expectedEvent);
            }
        }
    }
}
