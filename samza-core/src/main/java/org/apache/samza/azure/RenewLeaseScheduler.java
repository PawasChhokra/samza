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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RenewLeaseScheduler implements TaskScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(RenewLeaseScheduler.class);
  private static final long DELAY_IN_SEC = 45;
  private final ScheduledExecutorService scheduler;
  private SchedulerStateChangeListener listener = null;
  private LeaseBlobManager leaseBlobManager;
  String leaseId;

  public RenewLeaseScheduler(ScheduledExecutorService scheduler, LeaseBlobManager leaseBlobManager, String leaseId) {
    this.scheduler = scheduler;
    this.leaseBlobManager = leaseBlobManager;
    this.leaseId = leaseId;
  }

  @Override
  public ScheduledFuture scheduleTask() {
    return scheduler.scheduleWithFixedDelay(() -> {
        LOG.info("Renewing lease");
        boolean status = false;
        while (!status) {
          status = leaseBlobManager.renewLease(leaseId);
        }
      }, DELAY_IN_SEC, DELAY_IN_SEC, TimeUnit.SECONDS);
  }

  @Override
  public void setStateChangeListener(SchedulerStateChangeListener listener) {
    this.listener = listener;
  }
}
