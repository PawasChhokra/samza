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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.microsoft.azure.storage.blob.BlobProperties;
import java.util.Collection;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.samza.coordinator.LeaderElector;
import org.apache.samza.coordinator.LeaderElectorListener;
import org.apache.samza.zk.ZkLeaderElector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AzureLeaderElector implements LeaderElector {

  public static final Logger LOG = LoggerFactory.getLogger(ZkLeaderElector.class);
  private LeaderElectorListener leaderElectorListener = null;
  private final LeaseBlobManager leaseBlobManager;
  private static final int leaseTimeInSec = 60;
  private static final long length = 20000000;
  private final String leaseId;
  private final ScheduledExecutorService scheduler;
  private AtomicBoolean isLeader;

  public AzureLeaderElector(LeaseBlobManager leaseBlobManager, String leaseId) {
    this.isLeader = new AtomicBoolean(false);
    this.leaseBlobManager = leaseBlobManager;
    this.leaseId = leaseId;
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("AzureLeaderElector-%d").build();
    this.scheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
    this.isLeader = new AtomicBoolean(false);
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
    String id = leaseBlobManager.acquireLease(leaseTimeInSec, leaseId, length);
    if (id != null) {
      isLeader.set(true);
      leaderElectorListener.onBecomingLeader();
    }
  }

  /**
   * Release the lease
   */
  @Override
  public void resignLeadership() {
    boolean status = leaseBlobManager.releaseLease(leaseId);
    if (status) {
      isLeader.set(false);
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

}
