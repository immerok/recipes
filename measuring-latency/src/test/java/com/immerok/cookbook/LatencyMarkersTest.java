package com.immerok.cookbook;

import com.immerok.cookbook.extensions.MiniClusterExtensionFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.slf4j.Slf4jReporterFactory;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class LatencyMarkersTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK =
            MiniClusterExtensionFactory.withCustomConfiguration(getConfiguration());

    private static Configuration getConfiguration() {
        Configuration config = new Configuration();

        // WARNING: using latency markers can be very expensive
        config.set(MetricOptions.LATENCY_INTERVAL, 100L);
        config.setString(MetricOptions.LATENCY_SOURCE_GRANULARITY, "single");

        // Configure the slf4j metrics reporter
        config.setString(MetricOptions.REPORTERS_LIST, "slf4j");
        config.setString(
                "metrics.reporter.slf4j.factory.class", Slf4jReporterFactory.class.getName());
        config.setString("metrics.reporter.slf4j.interval", "10 SECONDS");

        // Only include the built-in latency metrics
        config.setString("metrics.reporter.slf4j.filter.includes", "*:*latency*");

        return config;
    }

    /**
     * Run the job and report the built-in latency metrics based on latency markers.
     *
     * <p>This test is a manual test because the job will never finish.
     */
    @Test
    @Disabled("Not running 'testReportLatencyMetrics()' because it is a manual test.")
    void testReportLatencyMetrics() throws Exception {
        TestJob.runUnbounded();
    }
}
