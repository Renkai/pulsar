/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import static org.junit.Assert.assertEquals;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.SegmentInfoImpl;
import org.junit.Test;
import org.testng.Assert;

public class BufferedOffloadStreamTest {
    final Random random = new Random();

    public void testWithPadding(int paddingLen) throws Exception {
        int blockSize = StreamingDataBlockHeaderImpl.getDataStartOffset();
        ConcurrentLinkedQueue<Entry> entryBuffer = new ConcurrentLinkedQueue<>();
        final UUID uuid = UUID.randomUUID();
        SegmentInfoImpl segmentInfo = new SegmentInfoImpl(uuid, 0, 0, "",
                new HashMap<>());
        AtomicLong bufferLength = new AtomicLong();
        final int entryCount = 10;
        List<Entry> entries = new ArrayList<>();
        for (int i = 0; i < entryCount; i++) {
            final byte[] bytes = new byte[random.nextInt(10)];
            final EntryImpl entry = EntryImpl.create(0, i, bytes);
            entries.add(entry);
            entry.retain();
            entryBuffer.add(entry);
            bufferLength.addAndGet(entry.getLength());
            blockSize += BufferedOffloadStream.ENTRY_HEADER_SIZE + entry.getLength();
        }
        segmentInfo.closeSegment(0, 9);
        blockSize += paddingLen;

        final BufferedOffloadStream inputStream = new BufferedOffloadStream(blockSize, entryBuffer,
                segmentInfo, segmentInfo.beginLedger,
                segmentInfo.beginEntry, bufferLength);
        assertEquals(inputStream.getLedgerId(), 0);
        assertEquals(inputStream.getBeginEntryId(), 0);
        assertEquals(inputStream.getBlockSize(), blockSize);

        byte headerB[] = new byte[DataBlockHeaderImpl.getDataStartOffset()];
        ByteStreams.readFully(inputStream, headerB);
        StreamingDataBlockHeaderImpl headerRead = StreamingDataBlockHeaderImpl
                .fromStream(new ByteArrayInputStream(headerB));
        Assert.assertEquals(headerRead.getBlockLength(), blockSize);
        Assert.assertEquals(headerRead.getFirstEntryId(), 0);

        int left = blockSize - DataBlockHeaderImpl.getDataStartOffset();
        for (int i = 0; i < entryCount; i++) {
            byte lengthBuf[] = new byte[4];
            byte entryIdBuf[] = new byte[8];
            byte content[] = new byte[entries.get(i).getLength()];

            left -= lengthBuf.length + entryIdBuf.length + content.length;
            inputStream.read(lengthBuf);
            inputStream.read(entryIdBuf);
            inputStream.read(content);
            Assert.assertEquals(entries.get(i).getLength(), Ints.fromByteArray(lengthBuf));
            Assert.assertEquals(i, Longs.fromByteArray(entryIdBuf));
            assertArrayEquals(entries.get(i).getData(), content);
        }
        assertEquals(left, paddingLen);
        byte padding[] = new byte[left];
        inputStream.read(padding);

        ByteBuf paddingBuf = Unpooled.wrappedBuffer(padding);
        for (int i = 0; i < paddingBuf.capacity() / 4; i++) {
            Assert.assertEquals(Integer.toHexString(paddingBuf.readInt()),
                    Integer.toHexString(0xFEDCDEAD));
        }

        // 4. reach end.
        Assert.assertEquals(inputStream.read(), -1);
        Assert.assertEquals(inputStream.read(), -1);
        assertEquals(bufferLength.get(), 0);
        inputStream.close();

    }

    @Test
    public void testHavePadding() throws Exception {
        testWithPadding(10);
    }

    @Test
    public void testNoPadding() throws Exception {
        testWithPadding(0);
    }

    @Test
    public void shouldEndWhenSegmentChanged() throws IOException {
        int blockSize = StreamingDataBlockHeaderImpl.getDataStartOffset();
        int paddingLen = 10;
        ConcurrentLinkedQueue<Entry> entryBuffer = new ConcurrentLinkedQueue<>();
        final UUID uuid = UUID.randomUUID();
        SegmentInfoImpl segmentInfo = new SegmentInfoImpl(uuid, 0, 0, "",
                new HashMap<>());
        AtomicLong bufferLength = new AtomicLong();
        final int entryCount = 10;
        List<Entry> entries = new ArrayList<>();
        for (int i = 0; i < entryCount; i++) {
            final byte[] bytes = new byte[random.nextInt(10)];
            final EntryImpl entry = EntryImpl.create(0, i, bytes);
            entries.add(entry);
            entry.retain();
            entryBuffer.add(entry);
            blockSize += BufferedOffloadStream.ENTRY_HEADER_SIZE + entry.getLength();
        }
        //create new ledger
        {
            final byte[] bytes = new byte[random.nextInt(10)];
            final EntryImpl entry = EntryImpl.create(1, 0, bytes);
            entries.add(entry);
            entry.retain();
            entryBuffer.add(entry);
        }
        segmentInfo.closeSegment(1, 0);
        blockSize += paddingLen;

        final BufferedOffloadStream inputStream = new BufferedOffloadStream(blockSize, entryBuffer,
                segmentInfo, segmentInfo.beginLedger,
                segmentInfo.beginEntry, bufferLength);
        assertEquals(inputStream.getLedgerId(), 0);
        assertEquals(inputStream.getBeginEntryId(), 0);
        assertEquals(inputStream.getBlockSize(), blockSize);

        byte headerB[] = new byte[DataBlockHeaderImpl.getDataStartOffset()];
        ByteStreams.readFully(inputStream, headerB);
        StreamingDataBlockHeaderImpl headerRead = StreamingDataBlockHeaderImpl
                .fromStream(new ByteArrayInputStream(headerB));
        Assert.assertEquals(headerRead.getBlockLength(), blockSize);
        Assert.assertEquals(headerRead.getFirstEntryId(), 0);

        int left = blockSize - DataBlockHeaderImpl.getDataStartOffset();
        for (int i = 0; i < entryCount; i++) {
            byte lengthBuf[] = new byte[4];
            byte entryIdBuf[] = new byte[8];
            byte content[] = new byte[entries.get(i).getLength()];

            left -= lengthBuf.length + entryIdBuf.length + content.length;
            inputStream.read(lengthBuf);
            inputStream.read(entryIdBuf);
            inputStream.read(content);
            Assert.assertEquals(entries.get(i).getLength(), Ints.fromByteArray(lengthBuf));
            Assert.assertEquals(i, Longs.fromByteArray(entryIdBuf));
            assertArrayEquals(entries.get(i).getData(), content);
        }
        assertEquals(left, paddingLen);
        byte padding[] = new byte[left];
        inputStream.read(padding);

        ByteBuf paddingBuf = Unpooled.wrappedBuffer(padding);
        for (int i = 0; i < paddingBuf.capacity() / 4; i++) {
            Assert.assertEquals(Integer.toHexString(paddingBuf.readInt()),
                    Integer.toHexString(0xFEDCDEAD));
        }

        // 4. reach end.
        Assert.assertEquals(inputStream.read(), -1);
        Assert.assertEquals(inputStream.read(), -1);
        inputStream.close();

        assertEquals(entryBuffer.size(), 1);
    }
}
