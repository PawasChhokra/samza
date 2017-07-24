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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


public class LivenessCheckScheduler implements TaskScheduler {

  private static final long CHECK_LIVENESS_DELAY = 30000;
  private static final String INITIAL_STATE = "unassigned";
  private final ScheduledExecutorService scheduler;
  private TableUtils table;
  private BlobUtils blob;
  private AtomicReference<String> currentJMVersion;
  private SchedulerStateChangeListener listener = null;
  private List<String> liveProcessorsList;

  public LivenessCheckScheduler(ScheduledExecutorService scheduler, AzureClient client, AtomicReference<String> currentJMVersion) {
    this.scheduler = scheduler;
    this.table = new TableUtils(client, "processors");
    this.blob = new BlobUtils(client, "testlease", "testblob", 5120000);
    this.currentJMVersion = currentJMVersion;
  }

  @Override
  public ScheduledFuture scheduleTask() {
    return scheduler.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        //check for change in list of processors
        Set<String> currProcessors = new HashSet<>(blob.getLiveProcessorList());
        Set<String> liveProcessors = getActiveProcessorsList();
        if (!liveProcessors.equals(currProcessors)) {
          liveProcessorsList = new ArrayList<>(liveProcessors);
          listener.onStateChange();
        }
      }
    }, CHECK_LIVENESS_DELAY, CHECK_LIVENESS_DELAY, TimeUnit.MILLISECONDS);
  }

  @Override
  public void setStateChangeListener(SchedulerStateChangeListener listener) {
    this.listener = listener;
  }

  /**
   *
   * @return List of ids of currently active processors in the application
   */
  private Set<String> getActiveProcessorsList() {
    Iterable<ProcessorEntity> tableList = table.getEntitiesWithPartition(currentJMVersion.get());
    Set<String> activeProcessorsList = new HashSet<>();
    for (ProcessorEntity entity: tableList) {
      if (System.currentTimeMillis() - entity.getTimestamp().getTime() <= CHECK_LIVENESS_DELAY) {
        activeProcessorsList.add(entity.getRowKey());
      }
    }

    Iterable<ProcessorEntity> unassignedList = table.getEntitiesWithPartition(INITIAL_STATE);
    for (ProcessorEntity entity: unassignedList) {
      if (System.currentTimeMillis() - entity.getTimestamp().getTime() <= CHECK_LIVENESS_DELAY) {
        activeProcessorsList.add(entity.getRowKey());
      }
    }
    return activeProcessorsList;
  }

  public List<String> getLiveProcessors() {
    return liveProcessorsList;
  }
}
