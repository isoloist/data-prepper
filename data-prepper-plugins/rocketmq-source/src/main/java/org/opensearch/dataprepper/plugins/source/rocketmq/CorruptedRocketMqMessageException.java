/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rocketmq;

class CorruptedRocketMqMessageException extends RuntimeException {
    CorruptedRocketMqMessageException(final String message) {
        super(message);
    }
}
