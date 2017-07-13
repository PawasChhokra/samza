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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
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
import org.apache.samza.zk.ZkJobCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AzureJobCoordinator implements JobCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(ZkJobCoordinator.class);
  private static final int METADATA_CACHE_TTL_MS = 5000;
  private final AzureLeaderElector azureLeaderElector;
  private final BlobUtils blobUtils;
  private StreamMetadataCache streamMetadataCache = null;
  private final Config config;
  private final String processorId;
  private JobCoordinatorListener coordinatorListener = null;

  private JobModel jobModel;
  private int debounceTimeMs;

  public AzureJobCoordinator(Config config) {
    this.config = config;
    this.processorId = createProcessorId(config);

    String storageConnectionString = "DefaultEndpointsProtocol=https;" + "AccountName=samzaonazure;"
        + "AccountKey=CTykRMBO0xCpyHXQNf02POGNnjcWyPVYkkX+VFmSLGKVI458a8SpqXldzD7YeGtJs415zdx3GIJasI/hLP8ccA==";

    this.blobUtils = new BlobUtils(storageConnectionString, "testlease", "testblob");
    this.azureLeaderElector = new AzureLeaderElector(new LeaseBlobManager(blobUtils.getBlobContainer(), blobUtils.getLeaseBlob()), processorId);
    this.debounceTimeMs = new JobConfig(config).getDebounceTimeMs();

  }

  @Override
  public void start() {
    streamMetadataCache = StreamMetadataCache.apply(METADATA_CACHE_TTL_MS, config);
    azureLeaderElector.tryBecomeLeader();
    if (azureLeaderElector.amILeader()) {
      //create job model
    } else {
      //heartbeat to the leader
    }

  }

  @Override
  public void stop() {
    if (coordinatorListener != null) {
      coordinatorListener.onJobModelExpired();
    }
    if (azureLeaderElector.amILeader()) {
      azureLeaderElector.resignLeadership();
    }

    if (coordinatorListener != null) {
      coordinatorListener.onCoordinatorStop();
    }
  }

  @Override
  public String getProcessorId() {
    return null;
  }

  @Override
  public void setListener(JobCoordinatorListener listener) {
    this.coordinatorListener = listener;
  }

  @Override
  public JobModel getJobModel() {
    return jobModel;
  }

  /**
   * Generate new JobModel when becoming a leader or the list of processor changed.
   */
  private JobModel generateNewJobModel(List<String> processors) {
    return JobModelManager.readJobModel(this.config, Collections.emptyMap(), null, streamMetadataCache,
        processors);
  }

  public void doOnProcessorChange(List<String> processors) {
    // if list of processors is empty - it means we are called from 'onBecomeLeader'
    List<String> currentProcessorIds = blobUtils.getActiveProcessorsList();

    // Generate the JobModel
    JobModel jobModel = generateNewJobModel(currentProcessorIds);
    // Assign the next version of JobModel
    String currentJMVersion  = blobUtils.getJobModelVersion();
    String nextJMVersion;
    if (currentJMVersion == null) {
      nextJMVersion = "1";
    } else {
      nextJMVersion = Integer.toString(Integer.valueOf(currentJMVersion) + 1);
    }
    LOG.info("pid=" + processorId + "Generated new Job Model. Version = " + nextJMVersion);

    // Publish the new job model
    blobUtils.publishJobModel(nextJMVersion, jobModel);
    blobUtils.publishJobModelVersion(currentJMVersion, nextJMVersion);
    LOG.info("pid=" + processorId + "Published new Job Model. Version = " + nextJMVersion);

    // Notify all processors about the new JobModel

    // Start the barrier for the job model update

  }

  public void onNewJobModelAvailable(final String version) {
    LOG.info("pid=" + processorId + "new JobModel available");

    // stop current work
    if (coordinatorListener != null) {
      coordinatorListener.onJobModelExpired();
    }
    // get the new job model from ZK
    jobModel = blobUtils.getJobModel(version);

    LOG.info("pid=" + processorId + ": new JobModel available. ver=" + version + "; jm = " + jobModel);

    // send notification to leader that you have the updated job model

  }

  public void onNewJobModelConfirmed(String version) {
    LOG.info("pid=" + processorId + "new version " + version + " of the job model got confirmed");
    // get the new Model
    JobModel newJobModel = getJobModel();

    // start the container with the new model
    if (coordinatorListener != null) {
      coordinatorListener.onNewJobModel(processorId, newJobModel);
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

    private final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("AzureLeaderElector-%d").build();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
    private static final long INITIAL_DELAY_IN_MILLISEC = 30000;
    private static final long DELAY_IN_MILLISEC = 30000;

    /**
     * Keep renewing the lease and do the required tasks as a leader
     */
    @Override
    public void onBecomingLeader() {
      scheduler.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          boolean status = false;
          while (!status) {
            status = azureLeaderElector.getLeaseBlobManager().renewLease(processorId);
          }
        }
      }, INITIAL_DELAY_IN_MILLISEC, DELAY_IN_MILLISEC, TimeUnit.MILLISECONDS);
    }

  }


}
