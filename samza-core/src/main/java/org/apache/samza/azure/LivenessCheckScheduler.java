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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Scheduler class for leader to check for changes in the list of live processors.
 */
public class LivenessCheckScheduler implements TaskScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(LivenessCheckScheduler.class);
  private static final long CHECK_LIVENESS_DELAY = 30;
  private final ScheduledExecutorService scheduler;
  private TableUtils table;
  private BlobUtils blob;
  private AtomicReference<String> currentJMVersion;
  private SchedulerStateChangeListener listener = null;
  private List<String> liveProcessorsList;

  public LivenessCheckScheduler(ScheduledExecutorService scheduler, TableUtils table, BlobUtils blob, AtomicReference<String> currentJMVersion) {
    this.scheduler = scheduler;
    this.table = table;
    this.blob = blob;
    this.currentJMVersion = currentJMVersion;
  }

  @Override
  public ScheduledFuture scheduleTask() {
    return scheduler.scheduleWithFixedDelay(() -> {
        //check for change in list of processors
        LOG.info("Checking for list of live processors");
        Set<String> currProcessors = new HashSet<>(blob.getLiveProcessorList());
        Set<String> liveProcessors = table.getActiveProcessorsList(currentJMVersion);
        if (!liveProcessors.equals(currProcessors)) {
          liveProcessorsList = new ArrayList<>(liveProcessors);
          listener.onStateChange();
        }
      }, CHECK_LIVENESS_DELAY * 4, CHECK_LIVENESS_DELAY, TimeUnit.SECONDS);
  }

  @Override
  public void setStateChangeListener(SchedulerStateChangeListener listener) {
    this.listener = listener;
  }

  public List<String> getLiveProcessors() {
    return liveProcessorsList;
  }
}
