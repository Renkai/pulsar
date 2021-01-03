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
import static org.testng.Assert.assertNotNull;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.LedgerOffloader.OffloaderHandle;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.JCloudBlobStoreProvider;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.jclouds.blobstore.BlobStore;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class BlobStoreManagedLedgerOffloaderStreamingTest extends BlobStoreManagedLedgerOffloaderBase {

    private static final Logger log = LoggerFactory.getLogger(BlobStoreManagedLedgerOffloaderStreamingTest.class);
    private TieredStorageConfiguration mockedConfig;

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
            offloaderHandle.offerEntry(EntryImpl.create(0, i, new byte[100]));
        }
        final LedgerOffloader.OffloadResult offloadResult = offloaderHandle.getOffloadResultAsync().get();
        log.info("Offload reasult: {}", offloadResult);
    }

    //TODO test write and read again

}
