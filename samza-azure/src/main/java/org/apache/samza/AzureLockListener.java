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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import org.apache.samza.coordinator.LockListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AzureLockListener implements LockListener {

  private static final Logger LOG = LoggerFactory.getLogger(AzureLockListener.class);
  private final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("AzureLockScheduler-%d").build();
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);

  public AzureLockListener() {

  }

  @Override
  public void onAcquiringLock() {
//    // Schedule a task to renew the lease after a fixed time interval
//    RenewLeaseScheduler renewLease = new RenewLeaseScheduler(scheduler, initLock.getLeaseBlobManager(), initLock.getLeaseId());
//    ScheduledFuture renewLeaseSF = renewLease.scheduleTask();
//    initLock.setRenewLeaseScheduledFuture(renewLeaseSF);
//
//    try {
//      createIntermediateStreams(intStreams);
//      LOG.info("Created intermediate streams successfully!");
//    } catch (Exception e) {
//      onError();
//    }
//
//    initLock.unlock();
  }

  @Override
  public void onError() {
    LOG.info("Error while creating streams! Trying again.");
  }
}
