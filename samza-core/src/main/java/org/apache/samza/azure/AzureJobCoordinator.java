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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorListener;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.LeaderElectorListener;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.runtime.ProcessorIdGenerator;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.util.ClassLoaderHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AzureJobCoordinator implements JobCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(AzureJobCoordinator.class);
  private static final int METADATA_CACHE_TTL_MS = 5000;
  private static final String BARRIER_STATE_START = "startbarrier_";
  public static final String BARRIER_STATE_END = "endbarrier_";
  private static final String INITIAL_STATE = "unassigned";
  private static final ThreadFactory PROCESSOR_THREAD_FACTORY = new ThreadFactoryBuilder().setNameFormat("AzureLeaderElector-%d").build();
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(20, PROCESSOR_THREAD_FACTORY);
  private final AzureLeaderElector azureLeaderElector;
  private final BlobUtils leaderBlob;
  private final TableUtils table;
  private final Config config;
  private final String processorId;
  private StreamMetadataCache streamMetadataCache = null;
  private JobCoordinatorListener coordinatorListener = null;
  private JobModel jobModel = null;
  private int debounceTimeMs;
  private AtomicReference<String> currentJMVersion = new AtomicReference<>(INITIAL_STATE);
  private AzureClient client;
  private ScheduledFuture heartbeatSF;
  private ScheduledFuture livenessSF;
  private ScheduledFuture leaderLivenessSF;
  private ScheduledFuture leaderBarrierSF;
  private ScheduledFuture workerBarrierSF;
  private ScheduledFuture versionUpgradeSF;
  private JMVersionUpgradeScheduler versionUpgrade;

  public AzureJobCoordinator(Config config) {
    this.config = config;
    this.processorId = createProcessorId(config);

    String storageConnectionString = "DefaultEndpointsProtocol=https;" + "AccountName=samzaonazure;"
        + "AccountKey=CTykRMBO0xCpyHXQNf02POGNnjcWyPVYkkX+VFmSLGKVI458a8SpqXldzD7YeGtJs415zdx3GIJasI/hLP8ccA==";
    client = new AzureClient(storageConnectionString);
    this.leaderBlob = new BlobUtils(client, "testlease", "testblob", 5120000);
    this.azureLeaderElector = new AzureLeaderElector(new LeaseBlobManager(leaderBlob.getBlobContainer(), leaderBlob.getBlob()));
    azureLeaderElector.setLeaderElectorListener(new AzureLeaderElectorListener());
    this.debounceTimeMs = new JobConfig(config).getDebounceTimeMs();
    this.table = new TableUtils(client, "processors");
  }

  @Override
  public void start() {

    streamMetadataCache = StreamMetadataCache.apply(METADATA_CACHE_TTL_MS, config);
    table.addProcessorEntity(INITIAL_STATE, processorId, 1, azureLeaderElector.amILeader());
    azureLeaderElector.tryBecomeLeader();

    // Start heartbeating
    HeartbeatScheduler heartbeat = new HeartbeatScheduler(scheduler, client, currentJMVersion, processorId);
    heartbeatSF = heartbeat.scheduleTask();

    // Check for job model version upgrade
    versionUpgrade = new JMVersionUpgradeScheduler(scheduler, client, currentJMVersion);
    versionUpgrade.setStateChangeListener(createJMVersionUpgradeListener());
    versionUpgradeSF = versionUpgrade.scheduleTask();

//    // Check if leader is alive
//    LeaderLivenessCheckScheduler leaderAlive = new LeaderLivenessCheckScheduler(scheduler, client, currentJMVersion);
//    leaderAlive.setStateChangeListener(createLeaderLivenessListener());
//    leaderLivenessSF = leaderAlive.scheduleTask();
  }

  @Override
  public void stop() {
    if (coordinatorListener != null) {
      coordinatorListener.onJobModelExpired();
    }
    if (azureLeaderElector.amILeader()) {
      azureLeaderElector.resignLeadership();
    }
    heartbeatSF.cancel(true);
    versionUpgradeSF.cancel(true);
//    leaderLivenessSF.cancel(true);

    if (coordinatorListener != null) {
      coordinatorListener.onCoordinatorStop();
    }
  }

  @Override
  public String getProcessorId() {
    return processorId;
  }

  @Override
  public void setListener(JobCoordinatorListener listener) {
    this.coordinatorListener = listener;
  }

  @Override
  public JobModel getJobModel() {
    return jobModel;
  }

  SchedulerStateChangeListener createLeaderLivenessListener() {
    return () -> azureLeaderElector.tryBecomeLeader();
  }

  SchedulerStateChangeListener createLivenessListener(List<String> liveProcessors) {
    return () -> doOnProcessorChange(liveProcessors);
  }

  SchedulerStateChangeListener createLeaderBarrierStateListener(String nextJMVersion) {
    return () -> {
      leaderBlob.publishBarrierState(BARRIER_STATE_END + nextJMVersion, azureLeaderElector.getLeaseId());
      leaderBarrierSF.cancel(true);
    };
  }

  SchedulerStateChangeListener createWorkerBarrierStateListener(String nextJMVersion) {
    return () -> {
      onNewJobModelConfirmed(nextJMVersion);
      workerBarrierSF.cancel(true);
      versionUpgrade.scheduleTask();
    };
  }

  SchedulerStateChangeListener createJMVersionUpgradeListener() {
    return () -> {
      onNewJobModelAvailable(leaderBlob.getJobModelVersion());
      versionUpgradeSF.cancel(true);
    };
  }

  /**
   * Generate new JobModel when becoming a leader or the list of processor changed.
   */
  private JobModel generateNewJobModel(List<String> processors) {
    return JobModelManager.readJobModel(this.config, Collections.emptyMap(), null, streamMetadataCache,
        processors);
  }

  // Called only by leader with the new list of live processors
  public void doOnProcessorChange(List<String> currentProcessorIds) {
    // if list of processors is empty - it means we are called from 'onBecomeLeader'

    // Generate the JobModel
    JobModel newJobModel = generateNewJobModel(currentProcessorIds);
    String nextJMVersion;
    if (currentProcessorIds.isEmpty()) {
      nextJMVersion = "1";
      currentProcessorIds = new ArrayList<>(table.getActiveProcessorsList(currentJMVersion));
    } else {
      //Check if existing barrier state and version are correct. If previous barrier not reached, previous barrier times out

      nextJMVersion = Integer.toString(Integer.valueOf(currentJMVersion.get()) + 1);
    }

    LOG.info("pid=" + processorId + "Generated new Job Model. Version = " + nextJMVersion);

    // Publish the new job model
    leaderBlob.publishJobModel(jobModel, newJobModel, currentJMVersion.get(), nextJMVersion, azureLeaderElector.getLeaseId());
    // Publish barrier state
    leaderBlob.publishBarrierState(BARRIER_STATE_START + nextJMVersion, azureLeaderElector.getLeaseId());
    // Publish list of processors this function was called with
    leaderBlob.publishLiveProcessorList(currentProcessorIds, azureLeaderElector.getLeaseId());
    LOG.info("pid=" + processorId + "Published new Job Model. Version = " + nextJMVersion);

    // Start scheduler to check if barrier reached
    LeaderBarrierStateScheduler barrierState = new LeaderBarrierStateScheduler(scheduler, client, nextJMVersion);
    barrierState.setStateChangeListener(createLeaderBarrierStateListener(nextJMVersion));
    leaderBarrierSF = barrierState.scheduleTask();

  }


  public void onNewJobModelAvailable(final String nextJMVersion) {
    LOG.info("pid=" + processorId + "new JobModel available");

    // stop current work
    if (coordinatorListener != null) {
      coordinatorListener.onJobModelExpired();
    }
    // get the new job model from blob
    jobModel = leaderBlob.getJobModel();

    table.addProcessorEntity(nextJMVersion, processorId, 1, azureLeaderElector.amILeader());
    if (table.getEntity(INITIAL_STATE, processorId) != null) {
      table.deleteProcessorEntity(INITIAL_STATE, processorId);
    }

    LOG.info("pid=" + processorId + ": new JobModel available. ver=" + currentJMVersion + "; jm = " + jobModel);

    //Schedule task to check for barrier state
    WorkerBarrierStateScheduler blobBarrierState = new WorkerBarrierStateScheduler(scheduler, client, nextJMVersion);
    blobBarrierState.setStateChangeListener(createWorkerBarrierStateListener(nextJMVersion));
    workerBarrierSF = blobBarrierState.scheduleTask();
  }

  //called when barrier ends
  public void onNewJobModelConfirmed(final String nextJMVersion) {
    LOG.info("pid=" + processorId + "new version " + nextJMVersion + " of the job model got confirmed");
    table.deleteProcessorEntity(currentJMVersion.get(), processorId);

    //Start heartbeating to new entry only when barrier reached. Changing the current job model version enables that since we are heartbeating to a row identified by the job model version.
    currentJMVersion.getAndSet(nextJMVersion);
    // start the container with the new model
    if (coordinatorListener != null) {
      coordinatorListener.onNewJobModel(processorId, jobModel);
    }
  }


  private String createProcessorId(Config config) {
    // TODO: This check to be removed after 0.13+
    ApplicationConfig appConfig = new ApplicationConfig(config);
    if (appConfig.getProcessorId() != null) {
      return appConfig.getProcessorId();
    } else if (StringUtils.isNotBlank(appConfig.getAppProcessorIdGeneratorClass())) {
      ProcessorIdGenerator idGenerator =
          ClassLoaderHelper.fromClassName(appConfig.getAppProcessorIdGeneratorClass(), ProcessorIdGenerator.class);
      return idGenerator.generateProcessorId(config);
    } else {
      throw new ConfigException(String
          .format("Expected either %s or %s to be configured", ApplicationConfig.PROCESSOR_ID,
              ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS));
    }
  }

  public class AzureLeaderElectorListener implements LeaderElectorListener {

    private static final long DELAY_IN_SEC = 45;

    /**
     * Keep renewing the lease and do the required tasks as a leader
     */
    @Override
    public void onBecomingLeader() {

      // TODO: Check if I need to stop leader aliveness task when I become leader
      // Update table

      table.updateIsLeader(currentJMVersion.get(), processorId, true);

      // Schedule a task to renew the lease after a fixed time interval
      ScheduledFuture sf = scheduler.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          LOG.info("Renewing lease");
          boolean status = false;
          while (!status) {
            status = azureLeaderElector.getLeaseBlobManager().renewLease(azureLeaderElector.getLeaseId());
          }
        }
      }, DELAY_IN_SEC, DELAY_IN_SEC, TimeUnit.SECONDS);
      azureLeaderElector.setRenewLeaseScheduledFuture(sf);

      doOnProcessorChange(new ArrayList<>());

      // Start scheduler to check for change in list of live processors
      LivenessCheckScheduler liveness = new LivenessCheckScheduler(scheduler, client, currentJMVersion);
      liveness.setStateChangeListener(createLivenessListener(liveness.getLiveProcessors()));
      livenessSF = liveness.scheduleTask();
      azureLeaderElector.setLivenessScheduledFuture(livenessSF);

    }

  }


}
