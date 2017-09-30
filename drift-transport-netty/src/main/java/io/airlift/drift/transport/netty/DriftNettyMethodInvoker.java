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
package io.airlift.drift.transport.netty;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.transport.AddressSelector;
import io.airlift.drift.transport.InvokeRequest;
import io.airlift.drift.transport.MethodInvoker;
import io.airlift.drift.transport.MethodMetadata;
import io.airlift.drift.transport.ResultClassification;
import io.airlift.drift.transport.ResultsClassifier;
import io.airlift.drift.transport.netty.ThriftClientHandler.ThriftRequest;
import io.netty.channel.Channel;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static java.util.Objects.requireNonNull;

class DriftNettyMethodInvoker
        implements MethodInvoker
{
    private final AddressSelector addressSelector;
    private final ConnectionManager connectionManager;
    private final ResultsClassifier globalResultsClassifier = new ResultsClassifier() {};

    public DriftNettyMethodInvoker(ConnectionManager connectionManager, AddressSelector addressSelector)
    {
        this.addressSelector = requireNonNull(addressSelector, "addressSelector is null");
        this.connectionManager = requireNonNull(connectionManager, "connectionManager is null");
    }

    @Override
    public ListenableFuture<Object> invoke(InvokeRequest request)
    {
        try {
            List<HostAndPort> addresses = addressSelector.getAddresses(request.getAddressSelectionContext());
            InvocationAttempt invocationAttempt = new InvocationAttempt(
                    addresses,
                    connectionManager,
                    new MethodInvocationFunction(request.getMethod(), request.getParameters(), globalResultsClassifier),
                    addressSelector::markdown);
            return invocationAttempt.getFuture();
        }
        catch (Exception e) {
            return immediateFailedFuture(e);
        }
    }

    private static class MethodInvocationFunction
            implements InvocationFunction<Channel>
    {
        private final MethodMetadata method;
        private final List<Object> parameters;
        private final ResultsClassifier globalResultsClassifier;

        public MethodInvocationFunction(MethodMetadata method, List<Object> parameters, ResultsClassifier globalResultsClassifier)
        {
            this.method = method;
            this.parameters = parameters;
            this.globalResultsClassifier = globalResultsClassifier;
        }

        @Override
        public ListenableFuture<Object> invokeOn(Channel channel)
        {
            try {
                ThriftRequest thriftRequest = new ThriftRequest(method, parameters);
                channel.writeAndFlush(thriftRequest);
                return thriftRequest;
            }
            catch (Throwable throwable) {
                return immediateFailedFuture(throwable);
            }
        }

        @Override
        public ResultClassification classifyResult(Object result)
        {
            ResultClassification methodClassification = method.getResultsClassifier().classifyResult(result);
            ResultClassification globalClassification = globalResultsClassifier.classifyResult(result);
            return merge(methodClassification, globalClassification);
        }

        @Override
        public ResultClassification classifyException(Throwable throwable)
        {
            ResultClassification methodClassification = method.getResultsClassifier().classifyException(throwable);
            ResultClassification globalClassification = globalResultsClassifier.classifyException(throwable);
            return merge(methodClassification, globalClassification);
        }

        private static ResultClassification merge(ResultClassification methodClassification, ResultClassification globalClassification)
        {
            Optional<Boolean> retry = methodClassification.isRetry();
            if (!retry.isPresent()) {
                retry = globalClassification.isRetry();
            }
            boolean hostDown = globalClassification.isHostDown() || methodClassification.isHostDown();
            return new ResultClassification(retry, hostDown);
        }
    }
}
