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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexEntry;
import org.apache.bookkeeper.mledger.offload.jcloud.StreamingOffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.StreamingOffloadIndexBlockBuilder;
import org.testng.annotations.Test;

@Slf4j
public class StreamingOffloadIndexTest {

    // prepare metadata, then use builder to build a StreamingOffloadIndexBlockImpl
    // verify get methods, readout and fromStream methods.
    @Test
    public void streamingOffloadIndexBlockImplTest() throws Exception {
        StreamingOffloadIndexBlockBuilder blockBuilder = StreamingOffloadIndexBlockBuilder.create();
        final long ledgerId = 1; // use dummy ledgerId, from BK 4.12 the ledger is is required
        LedgerMetadata metadata = OffloadIndexTest.createLedgerMetadata(ledgerId);
        log.debug("created metadata: {}", metadata.toString());

        blockBuilder.addLedgerMeta(ledgerId, metadata).withDataObjectLength(1).withDataBlockHeaderLength(23455);

        blockBuilder.addBlock(ledgerId, 0, 2, 64 * 1024 * 1024);
        blockBuilder.addBlock(ledgerId, 1000, 3, 64 * 1024 * 1024);
        blockBuilder.addBlock(ledgerId, 2000, 4, 64 * 1024 * 1024);
        StreamingOffloadIndexBlock indexBlock = blockBuilder.buildStreaming();

        // verify getEntryCount and getLedgerMetadata
        assertEquals(indexBlock.getEntryCount(), 3);
        assertEquals(indexBlock.getLedgerMetadata().get(ledgerId), metadata);

        // verify getIndexEntryForEntry
        OffloadIndexEntry entry1 = indexBlock.getIndexEntryForEntry(ledgerId, 0);
        assertEquals(entry1.getEntryId(), 0);
        assertEquals(entry1.getPartId(), 2);
        assertEquals(entry1.getOffset(), 0);

        OffloadIndexEntry entry11 = indexBlock.getIndexEntryForEntry(ledgerId, 500);
        assertEquals(entry11, entry1);

        OffloadIndexEntry entry2 = indexBlock.getIndexEntryForEntry(ledgerId, 1000);
        assertEquals(entry2.getEntryId(), 1000);
        assertEquals(entry2.getPartId(), 3);
        assertEquals(entry2.getOffset(), 64 * 1024 * 1024);

        OffloadIndexEntry entry22 = indexBlock.getIndexEntryForEntry(ledgerId, 1300);
        assertEquals(entry22, entry2);

        OffloadIndexEntry entry3 = indexBlock.getIndexEntryForEntry(ledgerId, 2000);

        assertEquals(entry3.getEntryId(), 2000);
        assertEquals(entry3.getPartId(), 4);
        assertEquals(entry3.getOffset(), 2 * 64 * 1024 * 1024);

        OffloadIndexEntry entry33 = indexBlock.getIndexEntryForEntry(ledgerId, 3000);
        assertEquals(entry33, entry3);

        try {
            OffloadIndexEntry entry4 = indexBlock.getIndexEntryForEntry(ledgerId, 6000);
            fail("Should throw IndexOutOfBoundsException.");
        } catch (Exception e) {
            assertTrue(e instanceof IndexOutOfBoundsException);
            assertEquals(e.getMessage(), "Entry index: 6000 beyond lastEntryId: 5000");
        }

        // verify toStream
        InputStream out = indexBlock.toStream();
        byte b[] = new byte[1024];
        int readoutLen = out.read(b);
        out.close();
        ByteBuf wrapper = Unpooled.wrappedBuffer(b);
        int magic = wrapper.readInt();
        int indexBlockLength = wrapper.readInt();
        long dataObjectLength = wrapper.readLong();
        long dataHeaderLength = wrapper.readLong();
        assertEquals(ledgerId, wrapper.readLong());
        int indexEntryCount = wrapper.readInt();
        int segmentMetadataLength = wrapper.readInt();

        // verify counter
        assertEquals(magic, StreamingOffloadIndexBlockImpl.getIndexMagicWord());
        assertEquals(indexBlockLength, readoutLen);
        assertEquals(indexEntryCount, 3);
        assertEquals(dataObjectLength, 1);
        assertEquals(dataHeaderLength, 23455);

        wrapper.readBytes(segmentMetadataLength);
        log.debug("magic: {}, blockLength: {}, metadataLength: {}, indexCount: {}",
                magic, indexBlockLength, segmentMetadataLength, indexEntryCount);

        // verify entry
        OffloadIndexEntry e1 = OffloadIndexEntryImpl.of(wrapper.readLong(), wrapper.readInt(),
                wrapper.readLong(), dataHeaderLength);
        OffloadIndexEntry e2 = OffloadIndexEntryImpl.of(wrapper.readLong(), wrapper.readInt(),
                wrapper.readLong(), dataHeaderLength);
        OffloadIndexEntry e3 = OffloadIndexEntryImpl.of(wrapper.readLong(), wrapper.readInt(),
                wrapper.readLong(), dataHeaderLength);

        assertEquals(e1.getEntryId(), entry1.getEntryId());
        assertEquals(e1.getPartId(), entry1.getPartId());
        assertEquals(e1.getOffset(), entry1.getOffset());
        assertEquals(e1.getDataOffset(), entry1.getDataOffset());
        assertEquals(e2.getEntryId(), entry2.getEntryId());
        assertEquals(e2.getPartId(), entry2.getPartId());
        assertEquals(e2.getOffset(), entry2.getOffset());
        assertEquals(e2.getDataOffset(), entry2.getDataOffset());
        assertEquals(e3.getEntryId(), entry3.getEntryId());
        assertEquals(e3.getPartId(), entry3.getPartId());
        assertEquals(e3.getOffset(), entry3.getOffset());
        assertEquals(e3.getDataOffset(), entry3.getDataOffset());
        wrapper.release();

        // verify build StreamingOffloadIndexBlock from InputStream
        InputStream out2 = indexBlock.toStream();
        int streamLength = out2.available();
        out2.mark(0);
        StreamingOffloadIndexBlock indexBlock2 = blockBuilder.streamingIndexFromStream(out2);
        // 1. verify metadata that got from inputstream success.
        LedgerMetadata metadata2 = indexBlock2.getLedgerMetadata().get(ledgerId);
        log.debug("built metadata: {}", metadata2.toString());
        assertEquals(metadata2.getAckQuorumSize(), metadata.getAckQuorumSize());
        assertEquals(metadata2.getEnsembleSize(), metadata.getEnsembleSize());
        assertEquals(metadata2.getDigestType(), metadata.getDigestType());
        assertEquals(metadata2.getAllEnsembles().entrySet(), metadata.getAllEnsembles().entrySet());
        // 2. verify set all the entries
        assertEquals(indexBlock2.getEntryCount(), indexBlock.getEntryCount());
        // 3. verify reach end
        assertEquals(out2.read(), -1);


        out2.reset();
        byte streamContent[] = new byte[streamLength];
        // stream with all 0, simulate junk data, should throw exception for header magic not match.
        try (InputStream stream3 = new ByteArrayInputStream(streamContent, 0, streamLength)) {
            StreamingOffloadIndexBlock indexBlock3 = blockBuilder.streamingIndexFromStream(stream3);
            fail("Should throw IOException");
        } catch (Exception e) {
            assertTrue(e instanceof IOException);
            assertTrue(e.getMessage().contains("Invalid MagicWord"));
        }

        // simulate read header too small, throw EOFException.
        out2.read(streamContent);
        try (InputStream stream4 =
                     new ByteArrayInputStream(streamContent, 0, streamLength - 1)) {
            StreamingOffloadIndexBlock indexBlock4 = blockBuilder.streamingIndexFromStream(stream4);
            fail("Should throw EOFException");
        } catch (Exception e) {
            assertTrue(e instanceof java.io.EOFException);
        }

        out2.close();
        indexBlock.close();
    }

    @Test
    public void streamingMultiLedgerOffloadIndexBlockImplTest() throws Exception {
        StreamingOffloadIndexBlockBuilder blockBuilder = StreamingOffloadIndexBlockBuilder.create();
        final long ledgerId1 = 1; // use dummy ledgerId, from BK 4.12 the ledger is is required
        final long ledgerId2 = 2;
        LedgerMetadata metadata1 = OffloadIndexTest.createLedgerMetadata(ledgerId1);
        LedgerMetadata metadata2 = OffloadIndexTest.createLedgerMetadata(ledgerId2);
        log.debug("created metadata: {}", metadata1.toString());
        log.debug("created metadata: {}", metadata2.toString());

        blockBuilder.addLedgerMeta(ledgerId1, metadata1)
                .addLedgerMeta(ledgerId2, metadata2)
                .withDataObjectLength(1)
                .withDataBlockHeaderLength(23455);

        blockBuilder.addBlock(ledgerId1, 1000, 2, 64 * 1024 * 1024);
        blockBuilder.addBlock(ledgerId2, 0, 3, 64 * 1024 * 1024);
        blockBuilder.addBlock(ledgerId2, 1000, 4, 64 * 1024 * 1024);
        StreamingOffloadIndexBlock indexBlock = blockBuilder.buildStreaming();

        // verify getEntryCount and getLedgerMetadata
        assertEquals(indexBlock.getEntryCount(), 3);
        assertEquals(indexBlock.getLedgerMetadata().get(ledgerId1), metadata1);
        assertEquals(indexBlock.getLedgerMetadata().get(ledgerId2), metadata2);

        // verify getIndexEntryForEntry
        OffloadIndexEntry entry1 = indexBlock.getIndexEntryForEntry(ledgerId1, 1000);
        assertEquals(entry1.getEntryId(), 1000);
        assertEquals(entry1.getPartId(), 2);
        assertEquals(entry1.getOffset(), 0);

        OffloadIndexEntry entry11 = indexBlock.getIndexEntryForEntry(ledgerId1, 1500);
        assertEquals(entry11, entry1);

        OffloadIndexEntry entry2 = indexBlock.getIndexEntryForEntry(ledgerId2, 0);
        assertEquals(entry2.getEntryId(), 0);
        assertEquals(entry2.getPartId(), 3);
        assertEquals(entry2.getOffset(), 64 * 1024 * 1024);

        OffloadIndexEntry entry22 = indexBlock.getIndexEntryForEntry(ledgerId2, 300);
        assertEquals(entry22, entry2);

        OffloadIndexEntry entry3 = indexBlock.getIndexEntryForEntry(ledgerId2, 1000);

        assertEquals(entry3.getEntryId(), 1000);
        assertEquals(entry3.getPartId(), 4);
        assertEquals(entry3.getOffset(), 2 * 64 * 1024 * 1024);

        OffloadIndexEntry entry33 = indexBlock.getIndexEntryForEntry(ledgerId2, 2000);
        assertEquals(entry33, entry3);

        try {
            OffloadIndexEntry entry4 = indexBlock.getIndexEntryForEntry(ledgerId2, 6000);
            fail("Should throw IndexOutOfBoundsException.");
        } catch (Exception e) {
            assertTrue(e instanceof IndexOutOfBoundsException);
            assertEquals(e.getMessage(), "Entry index: 6000 beyond lastEntryId: 5000");
        }

        // verify toStream
        InputStream out = indexBlock.toStream();
        byte b[] = new byte[1024];
        int readoutLen = out.read(b);
        out.close();
        ByteBuf wrapper = Unpooled.wrappedBuffer(b);
        int magic = wrapper.readInt();
        int indexBlockLength = wrapper.readInt();
        long dataObjectLength = wrapper.readLong();
        long dataHeaderLength = wrapper.readLong();
        assertEquals(ledgerId1, wrapper.readLong());
        int indexEntryCount = wrapper.readInt();
        int segmentMetadataLength = wrapper.readInt();

        // verify counter
        assertEquals(magic, StreamingOffloadIndexBlockImpl.getIndexMagicWord());
        assertEquals(indexBlockLength, readoutLen);
        assertEquals(indexEntryCount, 1);
        assertEquals(dataObjectLength, 1);
        assertEquals(dataHeaderLength, 23455);

        wrapper.readBytes(segmentMetadataLength);
        log.debug("magic: {}, blockLength: {}, metadataLength: {}, indexCount: {}",
                magic, indexBlockLength, segmentMetadataLength, indexEntryCount);

        // verify entry
        OffloadIndexEntry e1 = OffloadIndexEntryImpl.of(wrapper.readLong(), wrapper.readInt(),
                wrapper.readLong(), dataHeaderLength);

        assertEquals(e1.getEntryId(), entry1.getEntryId());
        assertEquals(e1.getPartId(), entry1.getPartId());
        assertEquals(e1.getOffset(), entry1.getOffset());
        assertEquals(e1.getDataOffset(), entry1.getDataOffset());


        assertEquals(ledgerId2, wrapper.readLong());
        int indexEntryCount2 = wrapper.readInt();
        assertEquals(indexEntryCount2, 2);
        int segmentMetadataLength2 = wrapper.readInt();
        wrapper.readBytes(segmentMetadataLength2);

        OffloadIndexEntry e2 = OffloadIndexEntryImpl.of(wrapper.readLong(), wrapper.readInt(),
                wrapper.readLong(), dataHeaderLength);
        OffloadIndexEntry e3 = OffloadIndexEntryImpl.of(wrapper.readLong(), wrapper.readInt(),
                wrapper.readLong(), dataHeaderLength);

        assertEquals(e2.getEntryId(), entry2.getEntryId());
        assertEquals(e2.getPartId(), entry2.getPartId());
        assertEquals(e2.getOffset(), entry2.getOffset());
        assertEquals(e2.getDataOffset(), entry2.getDataOffset());
        assertEquals(e3.getEntryId(), entry3.getEntryId());
        assertEquals(e3.getPartId(), entry3.getPartId());
        assertEquals(e3.getOffset(), entry3.getOffset());
        assertEquals(e3.getDataOffset(), entry3.getDataOffset());
        wrapper.release();

        // verify build StreamingOffloadIndexBlock from InputStream
        InputStream out2 = indexBlock.toStream();
        int streamLength = out2.available();
        out2.mark(0);
        StreamingOffloadIndexBlock indexBlock2 = blockBuilder.streamingIndexFromStream(out2);
        // 1. verify metadata that got from inputstream success.
        LedgerMetadata metadata1back = indexBlock2.getLedgerMetadata().get(ledgerId1);
        log.debug("built metadata: {}", metadata1back.toString());
        assertEquals(metadata1back.getAckQuorumSize(), metadata1.getAckQuorumSize());
        assertEquals(metadata1back.getEnsembleSize(), metadata1.getEnsembleSize());
        assertEquals(metadata1back.getDigestType(), metadata1.getDigestType());
        assertEquals(metadata1back.getAllEnsembles().entrySet(), metadata1.getAllEnsembles().entrySet());
        LedgerMetadata metadata2back = indexBlock2.getLedgerMetadata().get(ledgerId2);
        log.debug("built metadata: {}", metadata2back.toString());
        assertEquals(metadata2back.getAckQuorumSize(), metadata1.getAckQuorumSize());
        assertEquals(metadata2back.getEnsembleSize(), metadata1.getEnsembleSize());
        assertEquals(metadata2back.getDigestType(), metadata1.getDigestType());
        assertEquals(metadata2back.getAllEnsembles().entrySet(), metadata1.getAllEnsembles().entrySet());
        // 2. verify set all the entries
        assertEquals(indexBlock2.getEntryCount(), indexBlock.getEntryCount());
        // 3. verify reach end
        assertEquals(out2.read(), -1);

        out2.close();
        indexBlock.close();
    }
}
