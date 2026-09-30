/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rocketmq;

import java.util.Optional;

class RocketMqEnvironment {
    private final EnvironmentVariableProvider environmentVariableProvider;

    RocketMqEnvironment(final EnvironmentVariableProvider environmentVariableProvider) {
        this.environmentVariableProvider = environmentVariableProvider;
    }

    RocketMqCredentials loadCredentials(final RocketMqSourceConfig config) {
        return new RocketMqCredentials(
                readRequired(config.getCgEnvironmentVariable()),
                readRequired(config.getTokenEnvironmentVariable()),
                readRequired(config.getGroupEnvironmentVariable()));
    }

    private String readRequired(final String environmentVariable) {
        return Optional.ofNullable(environmentVariableProvider.getEnvironmentVariable(environmentVariable))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .orElseThrow(() -> new IllegalStateException(
                        "Missing required RocketMQ environment variable: " + environmentVariable));
    }
}
