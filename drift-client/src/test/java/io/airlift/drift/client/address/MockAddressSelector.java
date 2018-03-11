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

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.airlift.drift.transport.client.Address;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

public class MockAddressSelector
        implements AddressSelector<Address>
{
    private final List<HostAndPort> markdownHosts = new CopyOnWriteArrayList<>();
    private Optional<HostAndPort> address = Optional.of(HostAndPort.fromParts("localhost", 9999));

    public List<HostAndPort> getMarkdownHosts()
    {
        return ImmutableList.copyOf(markdownHosts);
    }

    public void setAddress(Optional<HostAndPort> address)
    {
        this.address = address;
    }

    @Override
    public Optional<Address> selectAddress(Optional<String> addressSelectionContext)
    {
        return address.map(hostAndPort -> () -> hostAndPort);
    }

    @Override
    public void markdown(Address address)
    {
        markdownHosts.add(address.getHostAndPort());
    }
}
