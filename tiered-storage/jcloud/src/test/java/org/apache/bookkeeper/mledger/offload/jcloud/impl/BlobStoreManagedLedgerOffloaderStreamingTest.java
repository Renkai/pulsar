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

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.LedgerOffloader.OffloaderHandle;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.JCloudBlobStoreProvider;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.OffloadContext;
import org.jclouds.blobstore.BlobStore;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class BlobStoreManagedLedgerOffloaderStreamingTest extends BlobStoreManagedLedgerOffloaderBase {

    private static final Logger log = LoggerFactory.getLogger(BlobStoreManagedLedgerOffloaderStreamingTest.class);
    private TieredStorageConfiguration mockedConfig;
    private static final Random random = new Random();

    BlobStoreManagedLedgerOffloaderStreamingTest() throws Exception {
        super();
        config = getConfiguration(BUCKET);
        JCloudBlobStoreProvider provider = getBlobStoreProvider();
        assertNotNull(provider);
        provider.validate(config);
        blobStore = provider.getBlobStore(config);
    }

    private BlobStoreManagedLedgerOffloader getOffloader(Map<String, String> additionalConfig) throws IOException {
        return getOffloader(BUCKET, additionalConfig);
    }

    private BlobStoreManagedLedgerOffloader getOffloader(BlobStore mockedBlobStore,
                                                         Map<String, String> additionalConfig) throws IOException {
        return getOffloader(BUCKET, mockedBlobStore, additionalConfig);
    }

    private BlobStoreManagedLedgerOffloader getOffloader(String bucket, Map<String, String> additionalConfig) throws
            IOException {
        mockedConfig = mock(TieredStorageConfiguration.class, delegatesTo(getConfiguration(bucket, additionalConfig)));
        Mockito.doReturn(blobStore).when(mockedConfig).getBlobStore(); // Use the REAL blobStore
        BlobStoreManagedLedgerOffloader offloader = BlobStoreManagedLedgerOffloader
                .create(mockedConfig, new HashMap<String, String>(), scheduler);
        return offloader;
    }

    private BlobStoreManagedLedgerOffloader getOffloader(String bucket, BlobStore mockedBlobStore,
                                                         Map<String, String> additionalConfig) throws IOException {
        mockedConfig = mock(TieredStorageConfiguration.class, delegatesTo(getConfiguration(bucket, additionalConfig)));
        Mockito.doReturn(mockedBlobStore).when(mockedConfig).getBlobStore();
        BlobStoreManagedLedgerOffloader offloader = BlobStoreManagedLedgerOffloader
                .create(mockedConfig, new HashMap<String, String>(), scheduler);
        return offloader;
    }

    @Test(timeOut = 10000)
    public void testHappyCase() throws Exception {
        LedgerOffloader offloader = getOffloader(new HashMap<String, String>() {{
            put(TieredStorageConfiguration.MAX_SEGMENT_SIZE_IN_BYTES, "1000");
            put(TieredStorageConfiguration.MAX_SEGMENT_TIME_IN_SECOND, "60000");
        }});
        ManagedLedger ml = createMockManagedLedger();
        UUID uuid = UUID.randomUUID();
        long beginLedger = 0;
        long beginEntry = 0;
        log.error("try begin offload");
        OffloaderHandle offloaderHandle = offloader
                .streamingOffload(ml, uuid, beginLedger, beginEntry, new HashMap<>()).get();

        //Segment should closed because size in bytes full
        for (int i = 0; i < 10; i++) {
            final byte[] data = new byte[100];
            random.nextBytes(data);
            offloaderHandle.offerEntry(EntryImpl.create(0, i, data));
        }
        final LedgerOffloader.OffloadResult offloadResult = offloaderHandle.getOffloadResultAsync().get();
        log.info("Offload reasult: {}", offloadResult);
    }

    @Test(timeOut = 10000)
    public void testReadAndWrite() throws Exception {
        LedgerOffloader offloader = getOffloader(new HashMap<String, String>() {{
            put(TieredStorageConfiguration.MAX_SEGMENT_SIZE_IN_BYTES, "1000");
            put(TieredStorageConfiguration.MAX_SEGMENT_TIME_IN_SECOND, "60000");
        }});
        ManagedLedger ml = createMockManagedLedger();
        UUID uuid = UUID.randomUUID();
        long beginLedger = 0;
        long beginEntry = 0;

        Map<String, String> driverMeta = new HashMap<String, String>() {{
            put(TieredStorageConfiguration.METADATA_FIELD_BUCKET, BUCKET);
        }};
        OffloaderHandle offloaderHandle = offloader
                .streamingOffload(ml, uuid, beginLedger, beginEntry, driverMeta).get();

        //Segment should closed because size in bytes full
        final LinkedList<Entry> entries = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            final byte[] data = new byte[100];
            random.nextBytes(data);
            final EntryImpl entry = EntryImpl.create(0, i, data);
            offloaderHandle.offerEntry(entry);
            entries.add(entry);
        }

        final LedgerOffloader.OffloadResult offloadResult = offloaderHandle.getOffloadResultAsync().get();
        assertEquals(offloadResult.endLedger, 0);
        assertEquals(offloadResult.endEntry, 9);
        final OffloadContext.Builder contextBuilder = OffloadContext.newBuilder();
        contextBuilder.addOffloadSegment(
                MLDataFormats.OffloadSegment.newBuilder()
                        .setUidLsb(uuid.getLeastSignificantBits())
                        .setUidMsb(uuid.getMostSignificantBits())
                        .setComplete(true).setEndEntryId(9).build());

        final ReadHandle readHandle = offloader.readOffloaded(0, contextBuilder.build(), driverMeta).get();
        final LedgerEntries ledgerEntries = readHandle.readAsync(0, 9).get();

        for (LedgerEntry ledgerEntry : ledgerEntries) {
            final EntryImpl storedEntry = (EntryImpl) entries.get((int) ledgerEntry.getEntryId());
            final byte[] storedData = storedEntry.getData();
            final byte[] entryBytes = ledgerEntry.getEntryBytes();
            assertEquals(storedData, entryBytes);
        }
    }

    @Test(timeOut = 10000)
    public void testReadAndWriteAcrossLedger() throws Exception {
        LedgerOffloader offloader = getOffloader(new HashMap<String, String>() {{
            put(TieredStorageConfiguration.MAX_SEGMENT_SIZE_IN_BYTES, "2000");
            put(TieredStorageConfiguration.MAX_SEGMENT_TIME_IN_SECOND, "60000");
        }});
        ManagedLedger ml = createMockManagedLedger();
        UUID uuid = UUID.randomUUID();
        long beginLedger = 0;
        long beginEntry = 0;

        Map<String, String> driverMeta = new HashMap<String, String>() {{
            put(TieredStorageConfiguration.METADATA_FIELD_BUCKET, BUCKET);
        }};
        OffloaderHandle offloaderHandle = offloader
                .streamingOffload(ml, uuid, beginLedger, beginEntry, driverMeta).get();

        //Segment should closed because size in bytes full
        final LinkedList<Entry> entries = new LinkedList<>();
        final LinkedList<Entry> ledger2Entries = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            final byte[] data = new byte[100];
            random.nextBytes(data);
            final EntryImpl entry = EntryImpl.create(0, i, data);
            offloaderHandle.offerEntry(entry);
            entries.add(entry);
        }
        for (int i = 0; i < 10; i++) {
            final byte[] data = new byte[100];
            random.nextBytes(data);
            final EntryImpl entry = EntryImpl.create(1, i, data);
            offloaderHandle.offerEntry(entry);
            ledger2Entries.add(entry);
        }

        final LedgerOffloader.OffloadResult offloadResult = offloaderHandle.getOffloadResultAsync().get();
        assertEquals(offloadResult.endLedger, 1);
        assertEquals(offloadResult.endEntry, 9);
        final OffloadContext.Builder contextBuilder = OffloadContext.newBuilder();
        contextBuilder.addOffloadSegment(
                MLDataFormats.OffloadSegment.newBuilder()
                        .setUidLsb(uuid.getLeastSignificantBits())
                        .setUidMsb(uuid.getMostSignificantBits())
                        .setComplete(true).setEndEntryId(9).build());

        final ReadHandle readHandle = offloader.readOffloaded(0, contextBuilder.build(), driverMeta).get();
        final LedgerEntries ledgerEntries = readHandle.readAsync(0, 9).get();

        for (LedgerEntry ledgerEntry : ledgerEntries) {
            final EntryImpl storedEntry = (EntryImpl) entries.get((int) ledgerEntry.getEntryId());
            final byte[] storedData = storedEntry.getData();
            final byte[] entryBytes = ledgerEntry.getEntryBytes();
            assertEquals(storedData, entryBytes);
        }

        final ReadHandle readHandle2 = offloader.readOffloaded(1, contextBuilder.build(), driverMeta).get();
        final LedgerEntries ledgerEntries2 = readHandle2.readAsync(0, 9).get();

        for (LedgerEntry ledgerEntry : ledgerEntries2) {
            final EntryImpl storedEntry = (EntryImpl) ledger2Entries.get((int) ledgerEntry.getEntryId());
            final byte[] storedData = storedEntry.getData();
            final byte[] entryBytes = ledgerEntry.getEntryBytes();
            assertEquals(storedData, entryBytes);
        }
    }

    @Test(timeOut = 10000)
    public void testReadAndWriteAcrossSegment() throws Exception {
        LedgerOffloader offloader = getOffloader(new HashMap<String, String>() {{
            put(TieredStorageConfiguration.MAX_SEGMENT_SIZE_IN_BYTES, "1000");
            put(TieredStorageConfiguration.MAX_SEGMENT_TIME_IN_SECOND, "60000");
        }});
        LedgerOffloader offloader2 = getOffloader(new HashMap<String, String>() {{
            put(TieredStorageConfiguration.MAX_SEGMENT_SIZE_IN_BYTES, "1000");
            put(TieredStorageConfiguration.MAX_SEGMENT_TIME_IN_SECOND, "60000");
        }});
        ManagedLedger ml = createMockManagedLedger();
        UUID uuid = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        long beginLedger = 0;
        long beginEntry = 0;

        Map<String, String> driverMeta = new HashMap<String, String>() {{
            put(TieredStorageConfiguration.METADATA_FIELD_BUCKET, BUCKET);
        }};
        OffloaderHandle offloaderHandle = offloader
                .streamingOffload(ml, uuid, beginLedger, beginEntry, driverMeta).get();

        //Segment should closed because size in bytes full
        final LinkedList<Entry> entries = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            final byte[] data = new byte[100];
            random.nextBytes(data);
            final EntryImpl entry = EntryImpl.create(0, i, data);
            offloaderHandle.offerEntry(entry);
            entries.add(entry);
        }

        final LedgerOffloader.OffloadResult offloadResult = offloaderHandle.getOffloadResultAsync().get();
        assertEquals(offloadResult.endLedger, 0);
        assertEquals(offloadResult.endEntry, 9);

        //Segment should closed because size in bytes full
        OffloaderHandle offloaderHandle2 = offloader2
                .streamingOffload(ml, uuid2, beginLedger, 10, driverMeta).get();
        for (int i = 0; i < 10; i++) {
            final byte[] data = new byte[100];
            random.nextBytes(data);
            final EntryImpl entry = EntryImpl.create(0, i + 10, data);
            offloaderHandle2.offerEntry(entry);
            entries.add(entry);
        }
        final LedgerOffloader.OffloadResult offloadResult2 = offloaderHandle2.getOffloadResultAsync().get();
        assertEquals(offloadResult2.endLedger, 0);
        assertEquals(offloadResult2.endEntry, 19);

        final OffloadContext.Builder contextBuilder = OffloadContext.newBuilder();
        contextBuilder.addOffloadSegment(
                MLDataFormats.OffloadSegment.newBuilder()
                        .setUidLsb(uuid.getLeastSignificantBits())
                        .setUidMsb(uuid.getMostSignificantBits())
                        .setComplete(true).setEndEntryId(9).build()).addOffloadSegment(
                MLDataFormats.OffloadSegment.newBuilder()
                        .setUidLsb(uuid2.getLeastSignificantBits())
                        .setUidMsb(uuid2.getMostSignificantBits())
                        .setComplete(true).setEndEntryId(19).build()
        );

        final ReadHandle readHandle = offloader.readOffloaded(0, contextBuilder.build(), driverMeta).get();
        final LedgerEntries ledgerEntries = readHandle.readAsync(0, 19).get();

        for (LedgerEntry ledgerEntry : ledgerEntries) {
            final EntryImpl storedEntry = (EntryImpl) entries.get((int) ledgerEntry.getEntryId());
            final byte[] storedData = storedEntry.getData();
            final byte[] entryBytes = ledgerEntry.getEntryBytes();
            assertEquals(storedData, entryBytes);
        }
    }
}
