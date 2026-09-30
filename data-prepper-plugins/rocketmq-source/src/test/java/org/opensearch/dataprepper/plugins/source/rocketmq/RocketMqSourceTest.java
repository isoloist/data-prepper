/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rocketmq;

import io.micrometer.core.instrument.Counter;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.acknowledgements.DefaultAcknowledgementSetManager;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.acknowledgements.ProgressCheck;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RocketMqSourceTest {
    private RocketMqSourceConfig config;
    private RocketMqEventMapper eventMapper;
    private AcknowledgementSetManager acknowledgementSetManager;
    private PluginMetrics pluginMetrics;
    private Counter counter;

    @BeforeEach
    void setUp() throws Exception {
        config = new RocketMqSourceConfig();
        setField("acknowledgements", false);
        eventMapper = mock(RocketMqEventMapper.class);
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        pluginMetrics = mock(PluginMetrics.class);
        counter = mock(Counter.class);
        when(pluginMetrics.counter(any())).thenReturn(counter);
    }

    @Test
    void processMessage_acknowledges_only_after_positive_end_to_end_acknowledgement() throws Exception {
        setField("acknowledgements", true);
        final Buffer<Record<Event>> buffer = mock(Buffer.class);
        final MessageView message = mockMessage(1);
        final Record<Event> record = new Record<>(JacksonEvent.fromMessage("ok"));
        final TestRocketMqConsumer consumer = new TestRocketMqConsumer();
        final AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Consumer<Boolean>> callbackCaptor =
                (ArgumentCaptor<Consumer<Boolean>>) (ArgumentCaptor) ArgumentCaptor.forClass(Consumer.class);
        when(eventMapper.toRecord(message)).thenReturn(record);
        when(acknowledgementSetManager.create(callbackCaptor.capture(), any(Duration.class), anyBoolean()))
                .thenReturn(acknowledgementSet);
        final RocketMqSource source = createSource(consumer);

        source.processMessage(buffer, message);

        assertThat(consumer.ackCount, equalTo(0));
        verify(acknowledgementSet).add(record.getData());
        verify(acknowledgementSet).complete();

        callbackCaptor.getValue().accept(true);

        assertThat(consumer.ackCount, equalTo(1));
        verify(acknowledgementSetManager).create(any(), any(Duration.class), eq(true));
    }

    @Test
    void processMessage_does_not_ack_after_negative_downstream_acknowledgement_before_max_attempts()
            throws Exception {
        setField("acknowledgements", true);
        final Buffer<Record<Event>> buffer = mock(Buffer.class);
        final MessageView message = mockMessage(config.getMaxDeliveryAttempts() - 1);
        final Record<Event> record = new Record<>(JacksonEvent.fromMessage("ok"));
        final TestRocketMqConsumer consumer = new TestRocketMqConsumer();
        final AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Consumer<Boolean>> callbackCaptor =
                (ArgumentCaptor<Consumer<Boolean>>) (ArgumentCaptor) ArgumentCaptor.forClass(Consumer.class);
        when(eventMapper.toRecord(message)).thenReturn(record);
        when(acknowledgementSetManager.create(callbackCaptor.capture(), any(Duration.class), anyBoolean()))
                .thenReturn(acknowledgementSet);
        final RocketMqSource source = createSource(consumer);

        source.processMessage(buffer, message);
        callbackCaptor.getValue().accept(false);

        assertThat(consumer.ackCount, equalTo(0));
    }

    @Test
    void processMessage_acknowledges_after_acknowledgement_set_expires_at_max_attempts()
            throws Exception {
        setField("acknowledgements", true);
        final Buffer<Record<Event>> buffer = mock(Buffer.class);
        final MessageView message = mockMessage(config.getMaxDeliveryAttempts());
        final Record<Event> record = new Record<>(JacksonEvent.fromMessage("ok"));
        final TestRocketMqConsumer consumer = new TestRocketMqConsumer();
        final AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Consumer<Boolean>> callbackCaptor =
                (ArgumentCaptor<Consumer<Boolean>>) (ArgumentCaptor) ArgumentCaptor.forClass(Consumer.class);
        when(eventMapper.toRecord(message)).thenReturn(record);
        when(acknowledgementSetManager.create(callbackCaptor.capture(), any(Duration.class), anyBoolean()))
                .thenReturn(acknowledgementSet);
        final RocketMqSource source = createSource(consumer);

        source.processMessage(buffer, message);
        callbackCaptor.getValue().accept(false);

        assertThat(consumer.ackCount, equalTo(1));
    }

    @Test
    void progressCheck_does_not_extend_message_invisibility_on_first_check()
            throws Exception {
        setField("acknowledgements", true);
        final Buffer<Record<Event>> buffer = mock(Buffer.class);
        final MessageView message = mockMessage(1);
        final Record<Event> record = new Record<>(JacksonEvent.fromMessage("ok"));
        final TestRocketMqConsumer consumer = new TestRocketMqConsumer();
        final AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Consumer<ProgressCheck>> progressCheckCaptor =
                (ArgumentCaptor<Consumer<ProgressCheck>>) (ArgumentCaptor) ArgumentCaptor.forClass(Consumer.class);
        when(eventMapper.toRecord(message)).thenReturn(record);
        when(acknowledgementSetManager.create(any(), any(Duration.class), anyBoolean()))
                .thenReturn(acknowledgementSet);
        final RocketMqSource source = createSource(consumer);

        source.processMessage(buffer, message);
        verify(acknowledgementSet).addProgressCheck(progressCheckCaptor.capture(), any(Duration.class));
        progressCheckCaptor.getValue().accept(mockProgressCheck(1.0));

        verify(acknowledgementSet, never()).increaseExpiry(any(Duration.class));
        assertThat(consumer.invisibilityChanged.await(100, TimeUnit.MILLISECONDS), is(false));
        assertThat(consumer.changeInvisibleDurationCount, equalTo(0));
        source.stop();
    }

    @Test
    void progressCheck_extends_acknowledgement_set_expiry_only_after_progress()
            throws Exception {
        setField("acknowledgements", true);
        final Buffer<Record<Event>> buffer = mock(Buffer.class);
        final MessageView message = mockMessage(1);
        final Record<Event> record = new Record<>(JacksonEvent.fromMessage("ok"));
        final TestRocketMqConsumer consumer = new TestRocketMqConsumer();
        final AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Consumer<ProgressCheck>> progressCheckCaptor =
                (ArgumentCaptor<Consumer<ProgressCheck>>) (ArgumentCaptor) ArgumentCaptor.forClass(Consumer.class);
        when(eventMapper.toRecord(message)).thenReturn(record);
        when(acknowledgementSetManager.create(any(), any(Duration.class), anyBoolean()))
                .thenReturn(acknowledgementSet);
        final RocketMqSource source = createSource(consumer);

        source.processMessage(buffer, message);
        verify(acknowledgementSet).addProgressCheck(progressCheckCaptor.capture(), any(Duration.class));
        progressCheckCaptor.getValue().accept(mockProgressCheck(1.0));
        progressCheckCaptor.getValue().accept(mockProgressCheck(1.0));

        verify(acknowledgementSet, never()).increaseExpiry(any(Duration.class));
        assertThat(consumer.invisibilityChanged.await(1, TimeUnit.SECONDS), is(true));
        assertThat(consumer.changeInvisibleDurationCount, equalTo(1));

        progressCheckCaptor.getValue().accept(mockProgressCheck(0.5));

        verify(acknowledgementSet).increaseExpiry(config.getAcknowledgementTimeout());
        source.stop();
    }

    @Test
    void processMessage_acknowledges_after_real_acknowledgement_set_expires_at_max_attempts()
            throws Exception {
        setField("acknowledgements", true);
        setField("acknowledgementTimeout", Duration.ofMillis(50));
        setField("invisibleDuration", Duration.ofSeconds(10));
        final ScheduledExecutorService callbackExecutor = Executors.newScheduledThreadPool(2);
        final DefaultAcknowledgementSetManager realAcknowledgementSetManager =
                new DefaultAcknowledgementSetManager(callbackExecutor, Duration.ofMillis(10));
        final Buffer<Record<Event>> buffer = mock(Buffer.class);
        final MessageView message = mockMessage(config.getMaxDeliveryAttempts());
        final Record<Event> record = new Record<>(JacksonEvent.fromMessage("ok"));
        final TestRocketMqConsumer consumer = new TestRocketMqConsumer();
        when(eventMapper.toRecord(message)).thenReturn(record);
        final RocketMqSource source = new RocketMqSource(
                config, eventMapper, realAcknowledgementSetManager, consumer, pluginMetrics);

        try {
            source.processMessage(buffer, message);

            assertThat(consumer.acked.await(1, TimeUnit.SECONDS), is(true));
            assertThat(consumer.ackCount, equalTo(1));
        } finally {
            source.stop();
            realAcknowledgementSetManager.shutdown();
            callbackExecutor.shutdownNow();
        }
    }

    @Test
    void processMessage_acknowledges_after_buffer_write_succeeds_when_end_to_end_acknowledgements_disabled()
            throws Exception {
        final Buffer<Record<Event>> buffer = mock(Buffer.class);
        final MessageView message = mockMessage(1);
        final Record<Event> record = new Record<>(JacksonEvent.fromMessage("ok"));
        final TestRocketMqConsumer consumer = new TestRocketMqConsumer();
        when(eventMapper.toRecord(message)).thenReturn(record);
        final RocketMqSource source = createSource(consumer);

        source.processMessage(buffer, message);

        verify(buffer).write(record, Math.toIntExact(config.getBufferTimeout().toMillis()));
        assertThat(consumer.ackCount, equalTo(1));
    }

    @Test
    void processMessage_does_not_ack_when_buffer_write_fails_before_max_attempts() throws Exception {
        final Buffer<Record<Event>> buffer = mock(Buffer.class);
        final MessageView message = mockMessage(1);
        final Record<Event> record = new Record<>(JacksonEvent.fromMessage("ok"));
        final TestRocketMqConsumer consumer = new TestRocketMqConsumer();
        when(eventMapper.toRecord(message)).thenReturn(record);
        doThrow(new TimeoutException("full")).when(buffer).write(any(), anyInt());
        final RocketMqSource source = createSource(consumer);

        source.processMessage(buffer, message);

        assertThat(consumer.ackCount, equalTo(0));
    }

    @Test
    void processMessage_acknowledges_after_max_attempts_when_buffer_write_keeps_failing() throws Exception {
        final Buffer<Record<Event>> buffer = mock(Buffer.class);
        final MessageView message = mockMessage(config.getMaxDeliveryAttempts());
        final Record<Event> record = new Record<>(JacksonEvent.fromMessage("ok"));
        final TestRocketMqConsumer consumer = new TestRocketMqConsumer();
        when(eventMapper.toRecord(message)).thenReturn(record);
        doThrow(new TimeoutException("full")).when(buffer).write(any(), anyInt());
        final RocketMqSource source = createSource(consumer);

        source.processMessage(buffer, message);

        assertThat(consumer.ackCount, equalTo(1));
    }

    @Test
    void processMessage_acknowledges_corrupted_message_without_retrying() throws Exception {
        final Buffer<Record<Event>> buffer = mock(Buffer.class);
        final MessageView message = mockMessage(1);
        final TestRocketMqConsumer consumer = new TestRocketMqConsumer();
        when(eventMapper.toRecord(message)).thenThrow(new CorruptedRocketMqMessageException("bad"));
        final RocketMqSource source = createSource(consumer);

        source.processMessage(buffer, message);

        assertThat(consumer.ackCount, equalTo(1));
        verify(buffer, never()).write(any(), anyInt());
    }

    @Test
    void stop_closes_consumer_to_unblock_receive_loop() throws Exception {
        setField("acknowledgements", false);
        setField("closeTimeout", Duration.ofSeconds(1));
        final Buffer<Record<Event>> buffer = mock(Buffer.class);
        final BlockingRocketMqConsumer consumer = new BlockingRocketMqConsumer();
        final RocketMqSource source = new RocketMqSource(
                config,
                name -> "value",
                eventMapper,
                acknowledgementSetManager,
                credentials -> consumer,
                pluginMetrics);

        source.start(buffer);
        assertThat(consumer.receiveStarted.await(1, TimeUnit.SECONDS), is(true));

        source.stop();

        assertThat(consumer.closed, is(true));
    }

    private RocketMqSource createSource(final RocketMqConsumer consumer) {
        return new RocketMqSource(config, eventMapper, acknowledgementSetManager, consumer, pluginMetrics);
    }

    private MessageView mockMessage(final int deliveryAttempt) {
        final MessageId messageId = mock(MessageId.class);
        when(messageId.toString()).thenReturn("message-id");
        final MessageView message = mock(MessageView.class);
        when(message.getMessageId()).thenReturn(messageId);
        when(message.getDeliveryAttempt()).thenReturn(deliveryAttempt);
        return message;
    }

    private ProgressCheck mockProgressCheck(final double ratio) {
        final ProgressCheck progressCheck = mock(ProgressCheck.class);
        when(progressCheck.getRatio()).thenReturn(ratio);
        return progressCheck;
    }

    private void setField(final String fieldName, final Object value) throws Exception {
        final Field field = RocketMqSourceConfig.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(config, value);
    }

    private static class TestRocketMqConsumer implements RocketMqConsumer {
        private int ackCount;
        private int changeInvisibleDurationCount;
        private final CountDownLatch acked = new CountDownLatch(1);
        private final CountDownLatch invisibilityChanged = new CountDownLatch(1);

        @Override
        public List<MessageView> receive(final int maxMessages, final Duration invisibleDuration) {
            return Collections.emptyList();
        }

        @Override
        public void ack(final MessageView message) {
            ackCount++;
            acked.countDown();
        }

        @Override
        public void changeInvisibleDuration(final MessageView message, final Duration invisibleDuration) {
            changeInvisibleDurationCount++;
            invisibilityChanged.countDown();
        }

        @Override
        public void close() {
        }
    }

    private static class BlockingRocketMqConsumer implements RocketMqConsumer {
        private final CountDownLatch receiveStarted = new CountDownLatch(1);
        private volatile boolean closed;

        @Override
        public List<MessageView> receive(final int maxMessages, final Duration invisibleDuration)
                throws ClientException {
            receiveStarted.countDown();
            while (!closed) {
                try {
                    Thread.sleep(10);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new ClientException("interrupted");
                }
            }
            return Collections.emptyList();
        }

        @Override
        public void ack(final MessageView message) {
        }

        @Override
        public void changeInvisibleDuration(final MessageView message, final Duration invisibleDuration) {
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }
    }
}
