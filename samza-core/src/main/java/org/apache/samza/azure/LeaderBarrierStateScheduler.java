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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Scheduler class for leader to check if barrier is completed.
 * The leader polls the Azure processor table in order to do this.
 */
public class LeaderBarrierStateScheduler implements TaskScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderBarrierStateScheduler.class);
  private static final long BARRIER_REACHED_DELAY = 15;
  private final ScheduledExecutorService scheduler;
  private TableUtils table;
  private BlobUtils blob;
  private String nextJMVersion;
  private SchedulerStateChangeListener listener = null;

  public LeaderBarrierStateScheduler(ScheduledExecutorService scheduler, AzureClient client, String nextJMVersion) {
    this.scheduler = scheduler;
    this.table = new TableUtils(client, "processors");
    this.blob = new BlobUtils(client, "testlease", "testblob", 5120000);
    this.nextJMVersion = nextJMVersion;
  }

  @Override
  public ScheduledFuture scheduleTask() {
    return scheduler.scheduleWithFixedDelay(() -> {
        LOG.info("Leader checking for barrier state");
        Iterable<ProcessorEntity> tableList = table.getEntitiesWithPartition(nextJMVersion);
        Set<String> tableProcessors = new HashSet<>();
        for (ProcessorEntity entity: tableList) {
          tableProcessors.add(entity.getRowKey());
        }
        Set<String> blobProcessorList = new HashSet<>(blob.getLiveProcessorList());
        if (blobProcessorList.equals(tableProcessors)) {
          listener.onStateChange();
        }
      }, BARRIER_REACHED_DELAY, BARRIER_REACHED_DELAY, TimeUnit.SECONDS);
  }

  @Override
  public void setStateChangeListener(SchedulerStateChangeListener listener) {
    this.listener = listener;
  }

}
