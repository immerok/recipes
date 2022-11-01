package com.immerok.cookbook.extensions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around a {@link MiniClusterWithClientResource} that allows it to be used as a JUnit5
 * Extension.
 *
 * <p>This class will be removed once 1.16.1 is released, containing FLINK-29693 that allows
 * controlling the default parallelism via the configuration of the MiniCluster.
 */
public class FlinkMiniClusterExtension implements BeforeAllCallback, AfterAllCallback {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkMiniClusterExtension.class);

    public static final int DEFAULT_PARALLELISM = 2;

    private static MiniClusterWithClientResource flinkCluster;

    private final Configuration config = new Configuration();

    public FlinkMiniClusterExtension() {}

    public FlinkMiniClusterExtension(Configuration config) {
        this.config.addAll(config);
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        flinkCluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                                .setConfiguration(config)
                                .setNumberTaskManagers(1)
                                .build());

        flinkCluster.before();
        TestStreamEnvironment.setAsContext(
                flinkCluster.getMiniCluster(),
                config.getOptional(CoreOptions.DEFAULT_PARALLELISM).orElse(DEFAULT_PARALLELISM));

        LOG.info("Web UI is available at {}", flinkCluster.getRestAddress());
    }

    @Override
    public void afterAll(ExtensionContext context) {
        flinkCluster.after();
    }
}
