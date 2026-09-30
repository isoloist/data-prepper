/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rocketmq;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.time.Duration;

public class RocketMqSourceConfig {
    static final String DEFAULT_CG_ENVIRONMENT_VARIABLE = "ROCKETMQ_CG";
    static final String DEFAULT_TOKEN_ENVIRONMENT_VARIABLE = "ROCKETMQ_TOKEN";
    static final String DEFAULT_GROUP_ENVIRONMENT_VARIABLE = "ROCKETMQ_GROUP";

    @JsonProperty("endpoints")
    @NotBlank
    private String endpoints;

    @JsonProperty("topic")
    @NotBlank
    private String topic;

    @JsonProperty("tag")
    @NotBlank
    private String tag = "*";

    @JsonProperty("cg_env")
    @NotBlank
    private String cgEnvironmentVariable = DEFAULT_CG_ENVIRONMENT_VARIABLE;

    @JsonProperty("token_env")
    @NotBlank
    private String tokenEnvironmentVariable = DEFAULT_TOKEN_ENVIRONMENT_VARIABLE;

    @JsonProperty("group_env")
    @NotBlank
    private String groupEnvironmentVariable = DEFAULT_GROUP_ENVIRONMENT_VARIABLE;

    @JsonProperty("max_messages")
    @Min(1)
    @Max(32)
    private int maxMessages = 16;

    @JsonProperty("max_delivery_attempts")
    @Min(1)
    private int maxDeliveryAttempts = 5;

    @JsonProperty("acknowledgments")
    private boolean acknowledgements = true;

    @JsonProperty("acknowledgment_timeout")
    private Duration acknowledgementTimeout = Duration.ofMinutes(2);

    @JsonProperty("request_timeout")
    private Duration requestTimeout = Duration.ofSeconds(30);

    @JsonProperty("await_duration")
    private Duration awaitDuration = Duration.ofSeconds(15);

    @JsonProperty("invisible_duration")
    private Duration invisibleDuration = Duration.ofSeconds(30);

    @JsonProperty("close_timeout")
    private Duration closeTimeout = Duration.ofSeconds(5);

    @JsonProperty("buffer_timeout")
    private Duration bufferTimeout = Duration.ofMillis(500);

    @JsonProperty("retry_delay")
    private Duration retryDelay = Duration.ofSeconds(5);

    public String getEndpoints() {
        return endpoints;
    }

    public String getTopic() {
        return topic;
    }

    public String getTag() {
        return tag;
    }

    public String getCgEnvironmentVariable() {
        return cgEnvironmentVariable;
    }

    public String getTokenEnvironmentVariable() {
        return tokenEnvironmentVariable;
    }

    public String getGroupEnvironmentVariable() {
        return groupEnvironmentVariable;
    }

    public int getMaxMessages() {
        return maxMessages;
    }

    public int getMaxDeliveryAttempts() {
        return maxDeliveryAttempts;
    }

    public boolean getAcknowledgements() {
        return acknowledgements;
    }

    public Duration getAcknowledgementTimeout() {
        return acknowledgementTimeout;
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    public Duration getAwaitDuration() {
        return awaitDuration;
    }

    public Duration getInvisibleDuration() {
        return invisibleDuration;
    }

    public Duration getCloseTimeout() {
        return closeTimeout;
    }

    public Duration getBufferTimeout() {
        return bufferTimeout;
    }

    public Duration getRetryDelay() {
        return retryDelay;
    }
}
