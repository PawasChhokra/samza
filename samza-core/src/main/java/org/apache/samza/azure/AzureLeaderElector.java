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

package org.apache.samza.azure;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.samza.coordinator.LeaderElector;
import org.apache.samza.coordinator.LeaderElectorListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AzureLeaderElector implements LeaderElector {

  private static final Logger LOG = LoggerFactory.getLogger(AzureLeaderElector.class);
  private static final int LEASE_TIME_IN_SEC = 60;
  private static final long LENGTH = 20000000;
  private final LeaseBlobManager leaseBlobManager;
  private LeaderElectorListener leaderElectorListener = null;
  private AtomicReference<String> leaseId;
  private AtomicBoolean isLeader;
  private ScheduledFuture renewLeaseSF;
  private ScheduledFuture livenessSF;

  public AzureLeaderElector(LeaseBlobManager leaseBlobManager) {
    this.isLeader = new AtomicBoolean(false);
    this.leaseBlobManager = leaseBlobManager;
    this.isLeader = new AtomicBoolean(false);
    this.leaseId = new AtomicReference<>(null);
  }

  @Override
  public void setLeaderElectorListener(LeaderElectorListener listener) {
    this.leaderElectorListener = listener;
  }

  /**
   * Try and acquire a lease on the shared blob.
   */
  @Override
  public void tryBecomeLeader() {
    leaseId.getAndSet(leaseBlobManager.acquireLease(LEASE_TIME_IN_SEC, leaseId.get(), LENGTH));
    if (leaseId != null) {
      LOG.info("Became leader!");
      isLeader.set(true);
      leaderElectorListener.onBecomingLeader();
    }
  }

  /**
   * Release the lease
   */
  @Override
  public void resignLeadership() {
    boolean status = leaseBlobManager.releaseLease(leaseId.get());
    if (status) {
      isLeader.set(false);
      leaseId = null;
      renewLeaseSF.cancel(true);
      livenessSF.cancel(true);
    }
  }

  /**
   * Check whether it's a leader
   * @return true if leader, false otherwise
   */
  @Override
  public boolean amILeader() {
    return isLeader.get();
  }

  public String getLeaseId() {
    return leaseId.get();
  }

  public void setRenewLeaseScheduledFuture(ScheduledFuture sf) {
    this.renewLeaseSF = sf;
  }

  public void setLivenessScheduledFuture(ScheduledFuture sf) {
    this.livenessSF = sf;
  }

  public LeaseBlobManager getLeaseBlobManager() {
    return this.leaseBlobManager;
  }

}
