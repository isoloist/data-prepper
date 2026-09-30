/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rocketmq;

import io.micrometer.core.instrument.Counter;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.opensearch.dataprepper.common.concurrent.BackgroundThreadFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@DataPrepperPlugin(name = "rocketmq", pluginType = Source.class, pluginConfigurationType = RocketMqSourceConfig.class)
public class RocketMqSource implements Source<Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(RocketMqSource.class);
    static final String MESSAGES_RECEIVED = "messagesReceived";
    static final String MESSAGES_WRITTEN = "messagesWritten";
    static final String MESSAGES_ACKNOWLEDGED = "messagesAcknowledged";
    static final String MESSAGES_FAILED = "messagesFailed";
    static final String MESSAGES_SKIPPED = "messagesSkipped";
    static final String CORRUPTED_MESSAGES = "corruptedMessages";
    static final String ACKNOWLEDGEMENT_SET_CALLBACKS = "acknowledgementSetCallbacks";

    private final RocketMqSourceConfig config;
    private final RocketMqEnvironment environment;
    private final RocketMqEventMapper eventMapper;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final RocketMqConsumerFactory consumerFactory;
    private final Counter messagesReceivedCounter;
    private final Counter messagesWrittenCounter;
    private final Counter messagesAcknowledgedCounter;
    private final Counter messagesFailedCounter;
    private final Counter messagesSkippedCounter;
    private final Counter corruptedMessagesCounter;
    private final Counter acknowledgementSetCallbacksCounter;
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);
    private ExecutorService executorService;
    private ExecutorService visibilityExtenderExecutorService;
    private RocketMqConsumer consumer;

    @DataPrepperPluginConstructor
    public RocketMqSource(final RocketMqSourceConfig config,
                          final PluginMetrics pluginMetrics,
                          final AcknowledgementSetManager acknowledgementSetManager) {
        this(config, EnvironmentVariableProvider.system(), new RocketMqEventMapper(), acknowledgementSetManager,
                new SimpleRocketMqConsumerFactory(config), pluginMetrics);
        if (!config.getAcknowledgements()) {
            LOG.warn("RocketMQ source acknowledgments are disabled. Messages will be acknowledged after they are " +
                    "written to the in-memory buffer, before delivery to sinks is confirmed. If the pipeline exits " +
                    "after receiving a message, that message can be lost.");
        }
    }

    RocketMqSource(final RocketMqSourceConfig config,
                   final EnvironmentVariableProvider environmentVariableProvider,
                   final RocketMqEventMapper eventMapper,
                   final AcknowledgementSetManager acknowledgementSetManager,
                   final RocketMqConsumerFactory consumerFactory,
                   final PluginMetrics pluginMetrics) {
        this.config = config;
        this.environment = new RocketMqEnvironment(environmentVariableProvider);
        this.eventMapper = eventMapper;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.consumerFactory = consumerFactory;
        this.messagesReceivedCounter = pluginMetrics.counter(MESSAGES_RECEIVED);
        this.messagesWrittenCounter = pluginMetrics.counter(MESSAGES_WRITTEN);
        this.messagesAcknowledgedCounter = pluginMetrics.counter(MESSAGES_ACKNOWLEDGED);
        this.messagesFailedCounter = pluginMetrics.counter(MESSAGES_FAILED);
        this.messagesSkippedCounter = pluginMetrics.counter(MESSAGES_SKIPPED);
        this.corruptedMessagesCounter = pluginMetrics.counter(CORRUPTED_MESSAGES);
        this.acknowledgementSetCallbacksCounter = pluginMetrics.counter(ACKNOWLEDGEMENT_SET_CALLBACKS);
    }

    RocketMqSource(final RocketMqSourceConfig config,
                   final RocketMqEventMapper eventMapper,
                   final AcknowledgementSetManager acknowledgementSetManager,
                   final RocketMqConsumer consumer,
                   final PluginMetrics pluginMetrics) {
        this(config, EnvironmentVariableProvider.system(), eventMapper, acknowledgementSetManager,
                credentials -> consumer, pluginMetrics);
        this.consumer = consumer;
    }

    @Override
    public boolean areAcknowledgementsEnabled() {
        return config.getAcknowledgements();
    }

    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        if (executorService != null) {
            throw new IllegalStateException("RocketMQ source has already started.");
        }
        stopRequested.set(false);
        final RocketMqCredentials credentials = environment.loadCredentials(config);
        consumer = consumerFactory.createConsumer(credentials);
        executorService = Executors.newSingleThreadExecutor(
                BackgroundThreadFactory.defaultExecutorThreadFactory("rocketmq-source"));
        executorService.submit(() -> consumeLoop(buffer));
        LOG.info("Started RocketMQ source for topic {}", config.getTopic());
    }

    @Override
    public void stop() {
        stopRequested.set(true);
        final boolean closed = closeConsumerWithTimeout();
        if (executorService != null) {
            executorService.shutdownNow();
            try {
                final long timeoutMillis = closed ? config.getCloseTimeout().toMillis() : 0;
                if (!executorService.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS)) {
                    LOG.warn("Timed out waiting for RocketMQ source worker to stop.");
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            executorService = null;
        }
        if (visibilityExtenderExecutorService != null) {
            visibilityExtenderExecutorService.shutdownNow();
            visibilityExtenderExecutorService = null;
        }
    }

    private void consumeLoop(final Buffer<Record<Event>> buffer) {
        while (!stopRequested.get()) {
            try {
                final List<MessageView> messages = consumer.receive(config.getMaxMessages(), config.getInvisibleDuration());
                messagesReceivedCounter.increment(messages.size());
                for (final MessageView message : messages) {
                    processMessage(buffer, message);
                }
            } catch (final ClientException e) {
                if (!stopRequested.get()) {
                    LOG.warn("Failed to receive RocketMQ messages. Retrying after {}.", config.getRetryDelay(), e);
                    sleepBeforeRetry();
                }
            }
        }
    }

    private boolean closeConsumerWithTimeout() {
        if (consumer == null) {
            return true;
        }
        final CountDownLatch closeCompleted = new CountDownLatch(1);
        final Thread closeThread = new Thread(() -> {
            try {
                consumer.close();
            } catch (final IOException e) {
                LOG.warn("Failed to close RocketMQ consumer.", e);
            } finally {
                closeCompleted.countDown();
            }
        }, "rocketmq-source-consumer-close");
        closeThread.setDaemon(true);
        closeThread.start();
        try {
            if (!closeCompleted.await(config.getCloseTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                LOG.warn("Timed out after {} waiting for RocketMQ consumer to close.", config.getCloseTimeout());
                return false;
            }
            return true;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    void processMessage(final Buffer<Record<Event>> buffer, final MessageView message) {
        try {
            if (config.getAcknowledgements()) {
                writeWithEndToEndAcknowledgement(buffer, message);
            } else {
                writeAndAckOnBufferWrite(buffer, message);
            }
        } catch (final TimeoutException e) {
            handleMessageProcessingFailure(message, "Timed out writing RocketMQ message to the buffer.", e);
        } catch (final RuntimeException e) {
            if (e instanceof CorruptedRocketMqMessageException) {
                handleCorruptedMessage(message, e);
                return;
            }
            handleMessageProcessingFailure(message, "Failed to map RocketMQ message.", e);
        } catch (final ClientException e) {
            handleMessageProcessingFailure(message, "Failed to acknowledge RocketMQ message.", e);
        }
    }

    private void writeWithEndToEndAcknowledgement(final Buffer<Record<Event>> buffer, final MessageView message)
            throws TimeoutException {
        AcknowledgementSet acknowledgementSet = null;
        try {
            final Record<Event> record = eventMapper.toRecord(message);
            acknowledgementSet = createAcknowledgementSet(message);
            acknowledgementSet.add(record.getData());
            buffer.write(record, bufferTimeoutMillis());
            messagesWrittenCounter.increment();
            acknowledgementSet.complete();
        } catch (final TimeoutException | RuntimeException e) {
            if (acknowledgementSet != null) {
                acknowledgementSet.cancel();
            }
            throw e;
        }
    }

    private AcknowledgementSet createAcknowledgementSet(final MessageView message) {
        final AcknowledgementSet acknowledgementSet = acknowledgementSetManager.create(result -> {
            acknowledgementSetCallbacksCounter.increment();
            if (result) {
                acknowledgeMessage(message, "downstream acknowledgement");
            } else {
                LOG.warn("RocketMQ message was negatively acknowledged or expired; messageId={}, deliveryAttempt={}.",
                        message.getMessageId(), message.getDeliveryAttempt());
                if (message.getDeliveryAttempt() >= config.getMaxDeliveryAttempts()) {
                    LOG.error("Skipping RocketMQ message after max delivery attempts following negative downstream " +
                                    "acknowledgement; messageId={}, deliveryAttempt={}, maxDeliveryAttempts={}.",
                            message.getMessageId(), message.getDeliveryAttempt(), config.getMaxDeliveryAttempts());
                    acknowledgeMessage(message, "max delivery attempts exceeded after negative downstream acknowledgement");
                }
            }
        }, config.getAcknowledgementTimeout(), true);

        final Duration progressCheckInterval = progressCheckInterval();
        final AtomicBoolean firstProgressCheck = new AtomicBoolean(true);
        final AtomicReference<Double> previousProgressRatio = new AtomicReference<>();
        acknowledgementSet.addProgressCheck(progressCheck -> {
            final Double progressRatio = progressCheck.getRatio();
            final Double previousRatio = previousProgressRatio.getAndSet(progressRatio);
            if (firstProgressCheck.getAndSet(false)) {
                return;
            }
            if (hasMadeProgress(previousRatio, progressRatio)) {
                acknowledgementSet.increaseExpiry(config.getAcknowledgementTimeout());
            }
            extendMessageInvisibility(message);
        }, progressCheckInterval);
        return acknowledgementSet;
    }

    private boolean hasMadeProgress(final Double previousRatio, final Double progressRatio) {
        return previousRatio != null && progressRatio != null
                && Double.isFinite(previousRatio) && Double.isFinite(progressRatio)
                && Double.compare(progressRatio, previousRatio) < 0;
    }

    private void extendMessageInvisibility(final MessageView message) {
        if (stopRequested.get()) {
            return;
        }
        try {
            getVisibilityExtenderExecutorService().submit(() -> {
                try {
                    consumer.changeInvisibleDuration(message, config.getInvisibleDuration());
                } catch (final ClientException e) {
                    LOG.warn("Failed to extend RocketMQ message invisibility; messageId={}, deliveryAttempt={}.",
                            message.getMessageId(), message.getDeliveryAttempt(), e);
                }
            });
        } catch (final RejectedExecutionException e) {
            if (!stopRequested.get()) {
                LOG.warn("Rejected RocketMQ message invisibility extension; messageId={}, deliveryAttempt={}.",
                        message.getMessageId(), message.getDeliveryAttempt(), e);
            }
        }
    }

    private synchronized ExecutorService getVisibilityExtenderExecutorService() {
        if (visibilityExtenderExecutorService == null || visibilityExtenderExecutorService.isShutdown()) {
            visibilityExtenderExecutorService = Executors.newFixedThreadPool(
                    Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors())),
                    BackgroundThreadFactory.defaultExecutorThreadFactory("rocketmq-source-visibility-extender"));
        }
        return visibilityExtenderExecutorService;
    }

    private Duration progressCheckInterval() {
        final long halfInvisibleMillis = config.getInvisibleDuration().toMillis() / 2;
        return Duration.ofMillis(Math.max(1000, halfInvisibleMillis));
    }

    private void writeAndAckOnBufferWrite(final Buffer<Record<Event>> buffer, final MessageView message)
            throws TimeoutException, ClientException {
        buffer.write(eventMapper.toRecord(message), bufferTimeoutMillis());
        messagesWrittenCounter.increment();
        consumer.ack(message);
        messagesAcknowledgedCounter.increment();
    }

    private int bufferTimeoutMillis() {
        return Math.toIntExact(config.getBufferTimeout().toMillis());
    }

    private void handleMessageProcessingFailure(final MessageView message, final String logMessage, final Exception e) {
        messagesFailedCounter.increment();
        if (message.getDeliveryAttempt() >= config.getMaxDeliveryAttempts()) {
            messagesSkippedCounter.increment();
            LOG.error("{} Skipping RocketMQ message after max delivery attempts; messageId={}, deliveryAttempt={}, " +
                            "maxDeliveryAttempts={}.",
                    logMessage, message.getMessageId(), message.getDeliveryAttempt(), config.getMaxDeliveryAttempts(), e);
            acknowledgeMessage(message, "max delivery attempts exceeded");
            return;
        }

        LOG.warn("{} RocketMQ message will be retried; messageId={}, deliveryAttempt={}, maxDeliveryAttempts={}.",
                logMessage, message.getMessageId(), message.getDeliveryAttempt(), config.getMaxDeliveryAttempts(), e);
    }

    private void handleCorruptedMessage(final MessageView message, final Exception e) {
        messagesFailedCounter.increment();
        messagesSkippedCounter.increment();
        corruptedMessagesCounter.increment();
        LOG.error("Skipping corrupted RocketMQ message; messageId={}, deliveryAttempt={}.",
                message.getMessageId(), message.getDeliveryAttempt(), e);
        acknowledgeMessage(message, "corrupted message");
    }

    private void acknowledgeMessage(final MessageView message, final String reason) {
        try {
            consumer.ack(message);
            messagesAcknowledgedCounter.increment();
        } catch (final ClientException e) {
            LOG.warn("Failed to acknowledge RocketMQ message after {}; messageId={}, deliveryAttempt={}.",
                    reason, message.getMessageId(), message.getDeliveryAttempt(), e);
        }
    }

    private void sleepBeforeRetry() {
        try {
            Thread.sleep(config.getRetryDelay().toMillis());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            stopRequested.set(true);
        }
    }
}
