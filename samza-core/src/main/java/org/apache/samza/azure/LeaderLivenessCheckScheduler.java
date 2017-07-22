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
import java.util.concurrent.atomic.AtomicReference;


public class LeaderLivenessCheckScheduler implements TaskScheduler {
  private final ScheduledExecutorService scheduler;
  private TableUtils table;
  private static final long CHECK_LIVENESS_DELAY = 30000;
  private AtomicReference<String> currentJMVersion;
  private SchedulerStateChangeListener listener = null;

  public LeaderLivenessCheckScheduler(ScheduledExecutorService scheduler, AzureClient client, AtomicReference<String> currentJMVersion) {
    this.scheduler = scheduler;
    this.table = new TableUtils(client, "processors");
    this.currentJMVersion = currentJMVersion;
  }

  @Override
  public ScheduledFuture scheduleTask() {
    return scheduler.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        if (!checkIfLeaderAlive()) {
          listener.onStateChange();
        }
      }
    }, CHECK_LIVENESS_DELAY, CHECK_LIVENESS_DELAY, TimeUnit.MILLISECONDS);
  }

  @Override
  public void setStateChangeListener(SchedulerStateChangeListener listener) {
    this.listener = listener;
  }

  private boolean checkIfLeaderAlive() {
    Iterable<ProcessorEntity> tableList = table.getEntitiesWithPartition(currentJMVersion.get());
    ProcessorEntity leader = null;
    for (ProcessorEntity entity: tableList) {
      if (entity.getIsLeader()) {
        leader = entity;
      }
    }
    if (System.currentTimeMillis() - leader.getTimestamp().getTime() >= CHECK_LIVENESS_DELAY) {
      return false;
    }
    return true;
  }

}
