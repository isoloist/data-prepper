/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rocketmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.pipeline.parser.DataPrepperDurationDeserializer;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class RocketMqSourceConfigTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
            .registerModule(new SimpleModule().addDeserializer(Duration.class, new DataPrepperDurationDeserializer()));

    @Test
    void parses_duration_strings_and_defaults() throws Exception {
        final String yaml = "endpoints: \"proxy:8081\"\n" +
                "topic: \"topic-a\"\n" +
                "await_duration: \"10s\"\n" +
                "invisible_duration: \"30s\"\n" +
                "close_timeout: \"3s\"\n";

        final RocketMqSourceConfig config = OBJECT_MAPPER.readValue(yaml, RocketMqSourceConfig.class);

        assertThat(config.getAwaitDuration(), equalTo(Duration.ofSeconds(10)));
        assertThat(config.getInvisibleDuration(), equalTo(Duration.ofSeconds(30)));
        assertThat(config.getCloseTimeout(), equalTo(Duration.ofSeconds(3)));
        assertThat(config.getAcknowledgements(), equalTo(true));
        assertThat(config.getMaxDeliveryAttempts(), equalTo(5));
    }

    @Test
    void await_duration_defaults_below_common_proxy_long_poll_limit() throws Exception {
        final String yaml = "endpoints: \"proxy:8081\"\n" +
                "topic: \"topic-a\"\n";

        final RocketMqSourceConfig config = OBJECT_MAPPER.readValue(yaml, RocketMqSourceConfig.class);

        assertThat(config.getAwaitDuration(), equalTo(Duration.ofSeconds(15)));
    }
}
