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
package io.airlift.drift.client.address;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkArgument;

public class SimpleAddressSelector
        implements AddressSelector
{
    private final Set<HostAndPort> addresses;

    public SimpleAddressSelector(SimpleAddressSelectorConfig config)
    {
        this(config.getAddresses());
    }

    public SimpleAddressSelector(Iterable<HostAndPort> addresses)
    {
        for (HostAndPort address : addresses) {
            checkArgument(address.getPortOrDefault(0) > 0, "address port must be set");
        }
        this.addresses = ImmutableSet.copyOf(addresses);
    }

    @Override
    public Optional<HostAndPort> selectAddress(Optional<String> addressSelectionContext)
    {
        checkArgument(!addressSelectionContext.isPresent(), "addressSelectionContext should not be set");
        List<HostAndPort> result = new ArrayList<>();
        for (HostAndPort address : addresses) {
            try {
                for (InetAddress ip : InetAddress.getAllByName(address.getHost())) {
                    result.add(HostAndPort.fromParts(ip.getHostAddress(), address.getPort()));
                }
            }
            catch (UnknownHostException ignored) {
            }
        }
        if (result.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(result.get(ThreadLocalRandom.current().nextInt(result.size())));
    }

    @Override
    public void markdown(HostAndPort address)
    {
        // TODO: implement some policy for blacklisting
    }
}
