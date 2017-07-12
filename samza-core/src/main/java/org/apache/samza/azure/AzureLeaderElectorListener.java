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

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.samza.coordinator.LeaderElectorListener;


public class AzureLeaderElectorListener implements LeaderElectorListener {

  private static final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("AzureLeaderElector-%d").build();
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
  private static final long initialDelayInMilliseconds = 60000;
  private static final long delayInMilliseconds = 60000;
  private final LeaseBlobManager leaseBlobManager;
  private String leaseId;

  public AzureLeaderElectorListener(LeaseBlobManager leaseBlobManager, String leaseId) {
    this.leaseBlobManager = leaseBlobManager;
    this.leaseId = leaseId;
  }

  /**
   * Keep renewing the lease and do the required tasks as a leader
   */
  @Override
  public void onBecomingLeader() {
    scheduler.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        boolean status = false;
        while(!status) {
          status = leaseBlobManager.renewLease(leaseId);
        }
      }
    }, initialDelayInMilliseconds, delayInMilliseconds, TimeUnit.MILLISECONDS);
  }

}
