/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rocketmq;

import org.apache.rocketmq.client.apis.message.MessageView;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

class RocketMqEventMapper {
    Record<Event> toRecord(final MessageView messageView) {
        if (isCorrupted(messageView)) {
            throw new CorruptedRocketMqMessageException("RocketMQ message is corrupted: " +
                    messageView.getMessageId());
        }
        final Map<String, Object> data = new HashMap<>();
        data.put("message", bodyAsString(messageView.getBody()));

        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("message_id", messageView.getMessageId().toString());
        metadata.put("topic", messageView.getTopic());
        messageView.getTag().ifPresent(tag -> metadata.put("tag", tag));
        metadata.put("keys", messageView.getKeys());
        metadata.put("properties", messageView.getProperties());
        data.put("rocketmq", metadata);

        return new Record<>(JacksonEvent.builder()
                .withEventType("ROCKETMQ")
                .withData(data)
                .build());
    }

    private String bodyAsString(final ByteBuffer body) {
        final ByteBuffer duplicate = body.asReadOnlyBuffer();
        final byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private boolean isCorrupted(final MessageView messageView) {
        try {
            final Method isCorrupted = messageView.getClass().getMethod("isCorrupted");
            return Boolean.TRUE.equals(isCorrupted.invoke(messageView));
        } catch (final NoSuchMethodException e) {
            return false;
        } catch (final IllegalAccessException e) {
            throw new CorruptedRocketMqMessageException("Unable to inspect RocketMQ message corruption status: " +
                    messageView.getMessageId());
        } catch (final InvocationTargetException e) {
            throw new CorruptedRocketMqMessageException("Unable to inspect RocketMQ message corruption status: " +
                    messageView.getMessageId());
        }
    }
}
