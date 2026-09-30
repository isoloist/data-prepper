/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rocketmq;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

class SimpleRocketMqConsumer implements RocketMqConsumer {
    private final SimpleConsumer simpleConsumer;

    SimpleRocketMqConsumer(final SimpleConsumer simpleConsumer) {
        this.simpleConsumer = simpleConsumer;
    }

    @Override
    public List<MessageView> receive(final int maxMessages, final Duration invisibleDuration) throws ClientException {
        return simpleConsumer.receive(maxMessages, invisibleDuration);
    }

    @Override
    public void ack(final MessageView message) throws ClientException {
        simpleConsumer.ack(message);
    }

    @Override
    public void changeInvisibleDuration(final MessageView message, final Duration invisibleDuration)
            throws ClientException {
        simpleConsumer.changeInvisibleDuration(message, invisibleDuration);
    }

    @Override
    public void close() throws IOException {
        simpleConsumer.close();
    }
}
