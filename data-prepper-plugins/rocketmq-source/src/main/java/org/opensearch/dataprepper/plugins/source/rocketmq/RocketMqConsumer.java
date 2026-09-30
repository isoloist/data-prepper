/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rocketmq;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageView;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

interface RocketMqConsumer {
    List<MessageView> receive(int maxMessages, Duration invisibleDuration) throws ClientException;

    void ack(MessageView message) throws ClientException;

    void changeInvisibleDuration(MessageView message, Duration invisibleDuration) throws ClientException;

    void close() throws IOException;
}
