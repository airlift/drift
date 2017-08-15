/*
 * Copyright (C) 2017 Facebook, Inc.
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

import static java.lang.Math.max;

public class TMemoryBuffer
        implements TTransport
{
    private byte[] data;
    private int head;
    private int tail;

    public TMemoryBuffer(int initialSize)
    {
        data = new byte[max(initialSize, 16)];
    }

    @Override
    public void read(byte[] buf, int off, int len)
            throws TTransportException
    {
        if (head - tail < len) {
            throw new TTransportException("Too few bytes in buffer");
        }
        System.arraycopy(data, tail, buf, off, len);
        tail += len;
    }

    @Override
    public void write(byte[] buf, int off, int len)
    {
        int available = data.length - head;
        if (available < len) {
            int need = len - available - tail;
            byte[] temp = new byte[max(data.length * 2, need)];
            System.arraycopy(data, tail, temp, 0, head - tail);
            data = temp;
            head -= tail;
            tail = 0;
        }
        System.arraycopy(buf, off, data, head, len);
        head += len;
    }
}
