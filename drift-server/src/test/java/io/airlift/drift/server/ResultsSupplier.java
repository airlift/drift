/*
 * Copyright (C) 2012 Facebook, Inc.
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
package io.airlift.drift.server;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.function.Supplier;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class ResultsSupplier
        implements Supplier<ListenableFuture<Object>>
{
    private ListenableFuture<Object> futureResult = immediateFuture(null);

    public void setSuccessResult(Object result)
    {
        this.futureResult = immediateFuture(result);
    }

    public void setFailedResult(Throwable throwable)
    {
        this.futureResult = immediateFailedFuture(throwable);
    }

    @Override
    public ListenableFuture<Object> get()
    {
        return futureResult;
    }
}
