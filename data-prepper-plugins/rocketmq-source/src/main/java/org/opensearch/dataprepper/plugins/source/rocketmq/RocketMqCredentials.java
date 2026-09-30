/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rocketmq;

import java.util.Objects;

class RocketMqCredentials {
    private final String accessKey;
    private final String token;
    private final String consumerGroup;

    RocketMqCredentials(final String accessKey, final String token, final String consumerGroup) {
        this.accessKey = Objects.requireNonNull(accessKey);
        this.token = Objects.requireNonNull(token);
        this.consumerGroup = Objects.requireNonNull(consumerGroup);
    }

    String getAccessKey() {
        return accessKey;
    }

    String getToken() {
        return token;
    }

    String getConsumerGroup() {
        return consumerGroup;
    }
}
