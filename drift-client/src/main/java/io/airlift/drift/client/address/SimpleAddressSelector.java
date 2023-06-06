/*
 * Copyright (C) 2012 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.drift.client.address;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.drift.client.address.SimpleAddressSelector.SimpleAddress;
import io.airlift.drift.transport.client.Address;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SimpleAddressSelector
        implements AddressSelector<SimpleAddress>
{
    private final Set<HostAndPort> addresses;
    private final boolean retrySameAddress;

    public SimpleAddressSelector(SimpleAddressSelectorConfig config)
    {
        this(config.getAddresses(), config.isRetrySameAddress());
    }

    public SimpleAddressSelector(Iterable<HostAndPort> addresses, boolean retrySameAddress)
    {
        for (HostAndPort address : addresses) {
            checkArgument(address.getPortOrDefault(0) > 0, "address port must be set");
        }
        this.addresses = ImmutableSet.copyOf(addresses);
        this.retrySameAddress = retrySameAddress;
    }

    @VisibleForTesting
    Set<HostAndPort> getAddresses()
    {
        return addresses;
    }

    @Override
    public Optional<SimpleAddress> selectAddress(Optional<String> addressSelectionContext, Set<SimpleAddress> attempted)
    {
        checkArgument(!addressSelectionContext.isPresent(), "addressSelectionContext should not be set");
        requireNonNull(attempted, "attempted is null");
        List<SimpleAddress> result = new ArrayList<>();
        for (HostAndPort address : addresses) {
            try {
                for (InetAddress ip : InetAddress.getAllByName(address.getHost())) {
                    SimpleAddress simpleAddress = new SimpleAddress(HostAndPort.fromParts(ip.getHostAddress(), address.getPort()));
                    if (retrySameAddress || !attempted.contains(simpleAddress)) {
                        result.add(simpleAddress);
                    }
                }
            }
            catch (UnknownHostException ignored) {
            }
        }
        if (result.isEmpty()) {
            return Optional.empty();
        }
        SimpleAddress address = result.get(ThreadLocalRandom.current().nextInt(result.size()));
        return Optional.of(address);
    }

    public static final class SimpleAddress
            implements Address
    {
        private final HostAndPort hostAndPort;

        public SimpleAddress(HostAndPort hostAndPort)
        {
            this.hostAndPort = requireNonNull(hostAndPort, "hostAndPort is null");
        }

        @Override
        public HostAndPort getHostAndPort()
        {
            return hostAndPort;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SimpleAddress that = (SimpleAddress) o;
            return Objects.equals(hostAndPort, that.hostAndPort);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(hostAndPort);
        }

        @Override
        public String toString()
        {
            return hostAndPort.toString();
        }
    }
}
