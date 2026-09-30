/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rocketmq;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RocketMqEnvironmentTest {
    @Test
    void loadCredentials_reads_values_from_configured_environment_variables() throws Exception {
        final RocketMqSourceConfig config = new RocketMqSourceConfig();
        setField(config, "cgEnvironmentVariable", "TEST_CG");
        setField(config, "tokenEnvironmentVariable", "TEST_TOKEN");
        setField(config, "groupEnvironmentVariable", "TEST_GROUP");
        final Map<String, String> values = Map.of(
                "TEST_CG", "CG-test",
                "TEST_TOKEN", "CT-test",
                "TEST_GROUP", "group-test");

        final RocketMqCredentials credentials = new RocketMqEnvironment(values::get).loadCredentials(config);

        assertThat(credentials.getAccessKey(), equalTo("CG-test"));
        assertThat(credentials.getToken(), equalTo("CT-test"));
        assertThat(credentials.getConsumerGroup(), equalTo("group-test"));
    }

    @Test
    void loadCredentials_throws_when_required_environment_variable_is_missing() {
        final RocketMqSourceConfig config = new RocketMqSourceConfig();

        final IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> new RocketMqEnvironment(name -> null).loadCredentials(config));

        assertThat(exception.getMessage(), equalTo("Missing required RocketMQ environment variable: ROCKETMQ_CG"));
    }

    private void setField(final RocketMqSourceConfig config, final String name, final Object value) throws Exception {
        final Field field = RocketMqSourceConfig.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(config, value);
    }
}
