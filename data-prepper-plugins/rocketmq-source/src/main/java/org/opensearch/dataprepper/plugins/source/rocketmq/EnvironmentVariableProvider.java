/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rocketmq;

@FunctionalInterface
interface EnvironmentVariableProvider {
    String getEnvironmentVariable(String name);

    static EnvironmentVariableProvider system() {
        return System::getenv;
    }
}
