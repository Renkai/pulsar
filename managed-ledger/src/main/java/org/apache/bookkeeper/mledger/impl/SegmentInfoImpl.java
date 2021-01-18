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
package org.apache.bookkeeper.mledger.impl;


import java.util.Map;
import java.util.UUID;
import lombok.ToString;
import org.apache.bookkeeper.mledger.LedgerOffloader;

@ToString
public class SegmentInfoImpl implements LedgerOffloader.SegmentInfo {
    public SegmentInfoImpl(UUID uuid, long beginLedger, long beginEntry, String driverName,
                           Map<String, String> driverMetadata) {
        this.uuid = uuid;
        this.beginLedger = beginLedger;
        this.beginEntry = beginEntry;
        this.driverName = driverName;
        this.driverMetadata = driverMetadata;
    }


    public final UUID uuid;
    public final long beginLedger;
    public final long beginEntry;
    public final String driverName;
    volatile private long endLedger;
    volatile private long endEntry;
    volatile boolean closed = false;
    public final long beginTimestamp = System.currentTimeMillis();
    public final Map<String, String> driverMetadata;

    public boolean isClosed() {
        return closed;
    }

    public void closeSegment(long endLedger, long endEntry) {
        this.endLedger = endLedger;
        this.endEntry = endEntry;
        this.closed = true;
    }

    public LedgerOffloader.OffloadResult result() {
        return new LedgerOffloader.OffloadResult(beginLedger, beginEntry, endLedger, endEntry);
    }
}
