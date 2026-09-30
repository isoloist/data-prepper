/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rocketmq;

interface RocketMqConsumerFactory {
    RocketMqConsumer createConsumer(RocketMqCredentials credentials);
}
