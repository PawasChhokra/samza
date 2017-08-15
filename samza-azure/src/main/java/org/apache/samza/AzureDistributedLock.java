/*
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

package org.apache.samza;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.samza.coordinator.DistributedLock;
import org.apache.samza.coordinator.LockListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AzureDistributedLock implements DistributedLock {

  private static final Logger LOG = LoggerFactory.getLogger(AzureDistributedLock.class);
  private static final int LEASE_TIME_IN_SEC = 60;
  private final BlobUtils blob;
  private AtomicBoolean hasLock;
  private AtomicReference<String> leaseId;
  private final LeaseBlobManager leaseBlobManager;
  private LockListener azureLockListener = null;
  private ScheduledFuture renewLeaseSF;

  public AzureDistributedLock(BlobUtils blobUtils) {
    this.blob = blobUtils;
    this.hasLock = new AtomicBoolean(false);
    leaseBlobManager = new LeaseBlobManager(blobUtils.getBlob());
    this.leaseId = new AtomicReference<>(null);

  }

  @Override
  public boolean lock(long timeout, TimeUnit unit) {
    //Start timer for timeout
    long startTime = System.currentTimeMillis();
    long lockTimeout = TimeUnit.MILLISECONDS.convert(timeout, unit);

    while ((System.currentTimeMillis() - startTime) < lockTimeout) {
      leaseId.getAndSet(leaseBlobManager.acquireLease(LEASE_TIME_IN_SEC, leaseId.get()));
      if (leaseId.get() != null) {
        LOG.info("Acquired lock!");
        hasLock.set(true);
        azureLockListener.onAcquiringLock();
        return true;
      } else {

      }
    }
    return false;
  }

  @Override
  public void unlock() {
    boolean status = leaseBlobManager.releaseLease(leaseId.get());
    if (status) {
      hasLock.set(false);
      leaseId = null;
      renewLeaseSF.cancel(true);
    }
  }

  public LeaseBlobManager getLeaseBlobManager() {
    return this.leaseBlobManager;
  }

  public String getLeaseId() {
    return leaseId.get();
  }

  public void setRenewLeaseScheduledFuture(ScheduledFuture sf) {
    this.renewLeaseSF = sf;
  }

}