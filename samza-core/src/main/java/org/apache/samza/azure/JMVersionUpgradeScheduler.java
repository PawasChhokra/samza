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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Scheduler for worker to check for job model version upgrades on the blob.
 */
public class JMVersionUpgradeScheduler implements TaskScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(JMVersionUpgradeScheduler.class);
  private static final long CHECK_UPGRADE_DELAY = 5;
  private final ScheduledExecutorService scheduler;
  private BlobUtils blob;
  private AtomicReference<String> currentJMVersion;
  private SchedulerStateChangeListener listener = null;

  public JMVersionUpgradeScheduler(ScheduledExecutorService scheduler, AzureClient client, AtomicReference<String> currentJMVersion) {
    this.scheduler = scheduler;
    this.blob = new BlobUtils(client, "testlease", "testblob", 5120000);
    this.currentJMVersion = currentJMVersion;
  }

  @Override
  public ScheduledFuture scheduleTask() {
    return scheduler.scheduleWithFixedDelay(() -> {
        LOG.info("Checking for job model version upgrade");
        String blobJMV = blob.getJobModelVersion();
        if (!currentJMVersion.get().equals(blobJMV)) {
          listener.onStateChange();
        }
      }, CHECK_UPGRADE_DELAY, CHECK_UPGRADE_DELAY, TimeUnit.SECONDS);
  }

  @Override
  public void setStateChangeListener(SchedulerStateChangeListener listener) {
    this.listener = listener;
  }

}
