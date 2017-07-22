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
  private final AzureLeaderElector azureLeaderElector;
  private final BlobUtils leaderBlob;
  private final TableUtils table;
  private StreamMetadataCache streamMetadataCache = null;
  private final Config config;
  private final String processorId;
  private JobCoordinatorListener coordinatorListener = null;
  private boolean isLeader;
  private JobModel jobModel = null;
  private int debounceTimeMs;
  private static final String BARRIER_STATE_START = "startbarrier_";
  private static final String BARRIER_STATE_END = "endbarrier_";
  private static final ThreadFactory PROCESSOR_THREAD_FACTORY = new ThreadFactoryBuilder().setNameFormat("AzureLeaderElector-%d").build();
  public final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(20, PROCESSOR_THREAD_FACTORY);

//  private static final long CHECK_LIVENESS_DELAY = 30;
//  private static final long HEARTBEAT_DELAY = 30;
//  private final long LIVENESS_DEBOUNCE_TIME = 30000;
  private static final String INITIAL_STATE = "unassigned";
  private AtomicReference<String> currentJMVersion = new AtomicReference<>("INITIAL_STATE");
  private AzureClient client;
  private AtomicReference<String> leaseId = new AtomicReference<>();
  ScheduledFuture heartbeatSF;
  ScheduledFuture livenessSF;
  ScheduledFuture leaderLivenessSF;

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
    isLeader = false;
  }

  @Override
  public void start() {

    streamMetadataCache = StreamMetadataCache.apply(METADATA_CACHE_TTL_MS, config);
    table.addProcessorEntity(INITIAL_STATE, processorId, 1, isLeader);
    azureLeaderElector.tryBecomeLeader();

    // Start heartbeating
    HeartbeatScheduler heartbeat = new HeartbeatScheduler(scheduler, client, currentJMVersion, processorId);
    heartbeatSF = heartbeat.scheduleTask();

//    // Check if leader is alive
//    LeaderLivenessCheckScheduler leaderAlive = new LeaderLivenessCheckScheduler(scheduler, client, currentJMVersion);
//    leaderAlive.setStateChangeListener(createLeaderLivenessListener());
//    leaderAlive.scheduleTask();
//    // Things to do if it becomes the leader
//    if (azureLeaderElector.amILeader()) {
//      isLeader = true;
//
//      // Start scheduler to check for change in list of live processors
//      LivenessCheckScheduler liveness = new LivenessCheckScheduler(scheduler, client, currentJMVersion);
//      liveness.setStateChangeListener(createLivenessListener(liveness.getLiveProcessors()));
//      liveness.scheduleTask();
//    }

  }

  @Override
  public void stop() {
    if (coordinatorListener != null) {
      coordinatorListener.onJobModelExpired();
    }
    if (isLeader) {
      livenessSF.cancel(true);
      azureLeaderElector.resignLeadership();
      leaseId.set(null);
    }
    heartbeatSF.cancel(true);
    leaderLivenessSF.cancel(true);

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
    return new SchedulerStateChangeListener() {
      @Override
      public void onStateChange() {
        azureLeaderElector.tryBecomeLeader();
      }
    };
  }

  SchedulerStateChangeListener createLivenessListener(List<String> liveProcessors) {
    return new SchedulerStateChangeListener() {
      @Override
      public void onStateChange() {
        doOnProcessorChange(liveProcessors);
      }
    };
  }

  public void checkIfJobModelUpdated() {
    String blobJMV = leaderBlob.getJobModelVersion();
    if (!currentJMVersion.get().equals(blobJMV)) {
//      prevJMVersion = currentJMVersion;
//      currentJMVersion = blobJMV;
      onNewJobModelAvailable(blobJMV);
    }
  }

  //Done by leader
  public void checkRebalancingState(String nextJMVersion) {
    Iterable<ProcessorEntity> tableList = table.getEntitiesWithPartition(nextJMVersion);
    Set<String> activeProcessorsList = new HashSet<>();
    for (ProcessorEntity entity: tableList) {
      activeProcessorsList.add(entity.getRowKey());
    }
    Set<String> blobProcessorList = new HashSet<>(leaderBlob.getLiveProcessorList());
    if (blobProcessorList.equals(activeProcessorsList)) {
      leaderBlob.publishBarrierState(BARRIER_STATE_END + currentJMVersion, leaseId.get());
    }
  }


  public void checkBarrierState(String nextJMVersion) {
    String waitingForState = BARRIER_STATE_END + nextJMVersion;
    String blobState = leaderBlob.getBarrierState();
    if (blobState.equals(waitingForState)) {
      onNewJobModelConfirmed(nextJMVersion);
    }
  }


  /**
   * Generate new JobModel when becoming a leader or the list of processor changed.
   */
  private JobModel generateNewJobModel(List<String> processors) {
    return JobModelManager.readJobModel(this.config, Collections.emptyMap(), null, streamMetadataCache,
        processors);
  }

  // Called only by leader
  public void doOnProcessorChange(List<String> currentProcessorIds) {
    // if list of processors is empty - it means we are called from 'onBecomeLeader'

    // Generate the JobModel
    JobModel newJobModel = generateNewJobModel(currentProcessorIds);
    String nextJMVersion;
    if (currentProcessorIds.isEmpty()) {
      nextJMVersion = "1";
//      currentJMVersion.getAndSet("1");
    } else {
      nextJMVersion = Integer.toString(Integer.valueOf(currentJMVersion.get()) + 1);
//      prevJMVersion = leaderBlob.getJobModelVersion();
//      currentJMVersion.getAndSet(Integer.toString(Integer.valueOf(prevJMVersion) + 1));
    }

    LOG.info("pid=" + processorId + "Generated new Job Model. Version = " + nextJMVersion);

    // Publish the new job model
    leaderBlob.publishJobModel(jobModel, newJobModel, currentJMVersion.get(), nextJMVersion, leaseId.get());
    // Publish barrier state
    leaderBlob.publishBarrierState(BARRIER_STATE_START + nextJMVersion, leaseId.get());
    // Publish list of processors this function was called with
    leaderBlob.publishLiveProcessorList(currentProcessorIds, leaseId.get());
    LOG.info("pid=" + processorId + "Published new Job Model. Version = " + nextJMVersion);

  }


  public void onNewJobModelAvailable(final String nextJMVersion) {
    LOG.info("pid=" + processorId + "new JobModel available");

    // stop current work
    if (coordinatorListener != null) {
      coordinatorListener.onJobModelExpired();
    }
    // get the new job model from blob
    jobModel = leaderBlob.getJobModel();

    table.addProcessorEntity(nextJMVersion, processorId, 1, isLeader);
    if (table.getEntity(INITIAL_STATE, processorId) != null) {
      table.deleteProcessorEntity(INITIAL_STATE, processorId);
    }

    LOG.info("pid=" + processorId + ": new JobModel available. ver=" + currentJMVersion + "; jm = " + jobModel);

    //check for barrier state
    checkBarrierState(nextJMVersion);
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

    private static final long DELAY_IN_SEC = 30;
    /**
     * Keep renewing the lease and do the required tasks as a leader
     */
    @Override
    public void onBecomingLeader() {
      leaseId.set(azureLeaderElector.getLeaseId());
      //Schedule a task to renew the lease after a fixed time interval
      ScheduledFuture sf = scheduler.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          boolean status = false;
          while (!status) {
            status = azureLeaderElector.getLeaseBlobManager().renewLease(leaseId.get());
          }
        }
      }, DELAY_IN_SEC, DELAY_IN_SEC, TimeUnit.SECONDS);
      azureLeaderElector.setRenewLeaseScheduledFuture(sf);
      doOnProcessorChange(new ArrayList<>());

    }

  }


}
