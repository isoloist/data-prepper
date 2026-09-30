/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rocketmq;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;

import java.util.Collections;

class SimpleRocketMqConsumerFactory implements RocketMqConsumerFactory {
    private final RocketMqSourceConfig config;

    SimpleRocketMqConsumerFactory(final RocketMqSourceConfig config) {
        this.config = config;
    }

    @Override
    public RocketMqConsumer createConsumer(final RocketMqCredentials credentials) {
        try {
            final ClientServiceProvider provider = ClientServiceProvider.loadService();
            final StaticSessionCredentialsProvider credentialsProvider =
                    new StaticSessionCredentialsProvider(credentials.getAccessKey(), credentials.getToken());
            final ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                    .setEndpoints(config.getEndpoints())
                    .setRequestTimeout(config.getRequestTimeout())
                    .setCredentialProvider(credentialsProvider)
                    .build();
            final FilterExpression filterExpression =
                    new FilterExpression(config.getTag(), FilterExpressionType.TAG);

            return new SimpleRocketMqConsumer(provider.newSimpleConsumerBuilder()
                    .setClientConfiguration(clientConfiguration)
                    .setConsumerGroup(credentials.getConsumerGroup())
                    .setAwaitDuration(config.getAwaitDuration())
                    .setSubscriptionExpressions(Collections.singletonMap(config.getTopic(), filterExpression))
                    .build());
        } catch (final ClientException e) {
            throw new IllegalStateException("Failed to initialize RocketMQ consumer.", e);
        }
    }
}
