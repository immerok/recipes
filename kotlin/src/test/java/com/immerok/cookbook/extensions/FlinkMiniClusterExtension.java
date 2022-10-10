package com.immerok.cookbook.extensions;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around a {@link MiniClusterWithClientResource} that allows it to be used as a JUnit5
 * Extension.
 */
public class FlinkMiniClusterExtension implements BeforeAllCallback, AfterAllCallback {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkMiniClusterExtension.class);

    private static final int PARALLELISM = 2;
    private static MiniClusterWithClientResource flinkCluster;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        flinkCluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberSlotsPerTaskManager(PARALLELISM)
                                .setNumberTaskManagers(1)
                                .build());

        flinkCluster.before();

        LOG.info("Web UI is available at {}", flinkCluster.getRestAddres());
    }

    @Override
    public void afterAll(ExtensionContext context) {
        flinkCluster.after();
    }
}
