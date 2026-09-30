/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rocketmq;

import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RocketMqEventMapperTest {
    @Test
    void toRecord_maps_body_and_metadata() {
        final MessageView message = mockMessage(false);

        final Record<Event> record = new RocketMqEventMapper().toRecord(message);
        final Event event = record.getData();

        assertThat(event.get("message", String.class), equalTo("hello"));
        assertThat(event.get("rocketmq/message_id", String.class), equalTo("message-id"));
        assertThat(event.get("rocketmq/topic", String.class), equalTo("topic-a"));
        assertThat(event.get("rocketmq/tag", String.class), equalTo("tag-a"));
        assertThat(event.get("rocketmq/properties/p1", String.class), equalTo("v1"));
    }

    @Test
    void toRecord_rejects_corrupted_message() {
        final MessageView message = mockMessage(true);

        assertThrows(CorruptedRocketMqMessageException.class,
                () -> new RocketMqEventMapper().toRecord(message));
    }

    private MessageView mockMessage(final boolean corrupted) {
        final MessageId messageId = mock(MessageId.class);
        when(messageId.toString()).thenReturn("message-id");
        return new TestMessageView(
                messageId,
                "topic-a",
                ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)),
                Map.of("p1", "v1"),
                Optional.of("tag-a"),
                List.of("key-a"),
                corrupted);
    }

    private static class TestMessageView implements MessageView {
        private final MessageId messageId;
        private final String topic;
        private final ByteBuffer body;
        private final Map<String, String> properties;
        private final Optional<String> tag;
        private final Collection<String> keys;
        private final boolean corrupted;

        private TestMessageView(final MessageId messageId,
                                final String topic,
                                final ByteBuffer body,
                                final Map<String, String> properties,
                                final Optional<String> tag,
                                final Collection<String> keys,
                                final boolean corrupted) {
            this.messageId = messageId;
            this.topic = topic;
            this.body = body;
            this.properties = properties;
            this.tag = tag;
            this.keys = keys;
            this.corrupted = corrupted;
        }

        public boolean isCorrupted() {
            return corrupted;
        }

        @Override
        public MessageId getMessageId() {
            return messageId;
        }

        @Override
        public String getTopic() {
            return topic;
        }

        @Override
        public ByteBuffer getBody() {
            return body.asReadOnlyBuffer();
        }

        @Override
        public Map<String, String> getProperties() {
            return properties;
        }

        @Override
        public Optional<String> getTag() {
            return tag;
        }

        @Override
        public Collection<String> getKeys() {
            return keys;
        }

        @Override
        public Optional<String> getMessageGroup() {
            return Optional.empty();
        }

        @Override
        public Optional<Long> getDeliveryTimestamp() {
            return Optional.empty();
        }

        @Override
        public String getBornHost() {
            return "host";
        }

        @Override
        public Optional<String> getTraceContext() {
            return Optional.empty();
        }

        @Override
        public long getBornTimestamp() {
            return 0;
        }

        @Override
        public int getDeliveryAttempt() {
            return 1;
        }
    }
}
