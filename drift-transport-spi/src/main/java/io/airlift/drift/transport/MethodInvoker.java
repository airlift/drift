/*
 * Copyright (C) 2013 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.airlift.drift.transport;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

public interface MethodInvoker
{
    /**
     * Invoke the specified method asynchronously.
     * <p>
     * If the invocation fails with a known application exception, the future will contain a
     * {@code DriftApplicationException} wrapper; otherwise, the future will contain the raw transport exception.
     */
    ListenableFuture<Object> invoke(InvokeRequest request);

    /**
     * Gets a future that completes after the specified delay.
     */
    ListenableFuture<?> delay(Duration duration);
}
