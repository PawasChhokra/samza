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

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudPageBlob;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import org.apache.samza.SamzaException;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BlobUtils {

  private static final Logger LOG = LoggerFactory.getLogger(BlobUtils.class);
  private static final long JOB_MODEL_BLOCK_SIZE = 1024000;
  private static final long BARRIER_STATE_BLOCK_SIZE = 1024;
  private static final long PROCESSOR_LIST_BLOCK_SIZE = 1024;
  private CloudBlobClient blobClient;
  private CloudBlobContainer container;
  private CloudPageBlob blob;

  public BlobUtils(AzureClient client, String containerName, String blobName, long length) {
    this.blobClient = client.getBlobClient();
    try {
      this.container = blobClient.getContainerReference(containerName);
      container.createIfNotExists();
      this.blob = container.getPageBlobReference(blobName);
      if (!blob.exists()) {
        blob.create(length);
      }
    } catch (URISyntaxException e) {
      LOG.error("Connection string specifies an invalid URI for Azure.", e);
    } catch (StorageException e) {
      LOG.error("Azure Storage Exception!", e);
    }
  }

  public void publishJobModel(JobModel prevJM, JobModel currJM, String prevJMV, String currJMV, String leaseId) {
    try {
      JobModelBundle bundle = new JobModelBundle(prevJM, currJM, prevJMV, currJMV);
      byte[] data = SamzaObjectMapper.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsBytes(bundle);
      System.out.println("Actual data length: " + data.length);
      byte[] pageData = Arrays.copyOf(data, (int) JOB_MODEL_BLOCK_SIZE);
      System.out.println("Padded data length: " + pageData.length);
      InputStream is = new ByteArrayInputStream(pageData);
      blob.uploadPages(is, 0, JOB_MODEL_BLOCK_SIZE, AccessCondition.generateLeaseCondition(leaseId), null, null);
      LOG.info("Uploaded jobModel to blob");

    } catch (Exception e) {
      LOG.error("JobModel publish failed for version=" + currJMV, e);
      throw new SamzaException(e);
    }
  }

  private JobModelBundle getJobModelBundle() {
    byte[] data = new byte[(int) JOB_MODEL_BLOCK_SIZE];
    try {
      blob.downloadRangeToByteArray(0, JOB_MODEL_BLOCK_SIZE, data, 0);
    } catch (StorageException e) {
      LOG.error("Azure Storage Exception!", e);
    }
    try {
      JobModelBundle jmBundle = SamzaObjectMapper.getObjectMapper().readValue(data, JobModelBundle.class);
      return jmBundle;
    } catch (IOException e) {
      LOG.error("failed to read JobModel details from the blob", new SamzaException(e));
    }
    return null;
  }

  public JobModel getJobModel() {
    LOG.info("Reading the job model from blob.");
    JobModelBundle jmBundle = getJobModelBundle();
    if (jmBundle == null) {
      LOG.error("Job Model details don't exist on the blob.");
      return null;
    }
    JobModel jm = jmBundle.getCurrJobModel();
    if (jm == null) {
      LOG.error("Job Model doesn't exist on the blob.");
    }
    return jm;
  }

  public String getJobModelVersion() {
    LOG.info("Reading the job model version from blob.");
    JobModelBundle jmBundle = getJobModelBundle();
    if (jmBundle == null) {
      LOG.error("Job Model details don't exist on the blob.");
      return null;
    }
    String jmVersion = jmBundle.getCurrJobModelVersion();
    if (jmVersion == null) {
      LOG.error("Job Model version doesn't exist on the blob.");
    }
    return jmVersion;
  }

  public void publishBarrierState(String state, String leaseId) {
    try {
      byte[] data = SamzaObjectMapper.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsBytes(state);
      byte[] pageData = Arrays.copyOf(data, (int) BARRIER_STATE_BLOCK_SIZE);
      InputStream is = new ByteArrayInputStream(pageData);
      blob.uploadPages(is, JOB_MODEL_BLOCK_SIZE, BARRIER_STATE_BLOCK_SIZE, AccessCondition.generateLeaseCondition(leaseId), null, null);
      LOG.info("Uploaded barrier state to blob");
    } catch (Exception e) {
      LOG.error("Barrier state publish failed", new SamzaException(e));
    }
  }

  public String getBarrierState() {
    LOG.info("Reading the barrier state from blob.");
    byte[] data = new byte[(int) BARRIER_STATE_BLOCK_SIZE];
    try {
      blob.downloadRangeToByteArray(JOB_MODEL_BLOCK_SIZE, BARRIER_STATE_BLOCK_SIZE, data, 0);
    } catch (StorageException e) {
      LOG.error("Azure Storage Exception!", e);
      return null;
    }
    String state = null;
    try {
      state = SamzaObjectMapper.getObjectMapper().readValue(data, String.class);
    } catch (IOException e) {
      LOG.error("Failed to read barrier state from blob", new SamzaException(e));
    }
    return state;
  }

  public void publishLiveProcessorList(List<String> processors, String leaseId) {
    try {
      byte[] data = SamzaObjectMapper.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsBytes(processors);
      byte[] pageData = Arrays.copyOf(data, (int) BARRIER_STATE_BLOCK_SIZE);
      InputStream is = new ByteArrayInputStream(pageData);
      blob.uploadPages(is, JOB_MODEL_BLOCK_SIZE + BARRIER_STATE_BLOCK_SIZE, PROCESSOR_LIST_BLOCK_SIZE, AccessCondition.generateLeaseCondition(leaseId), null, null);
      LOG.info("Uploaded list of live processors to blob.");
    } catch (Exception e) {
      LOG.error("Barrier state publish failed", new SamzaException(e));
    }
  }

  public List<String> getLiveProcessorList() {
    LOG.info("Read the the list of live processors from blob.");
    byte[] data = new byte[(int) PROCESSOR_LIST_BLOCK_SIZE];
    try {
      blob.downloadRangeToByteArray(JOB_MODEL_BLOCK_SIZE + BARRIER_STATE_BLOCK_SIZE, PROCESSOR_LIST_BLOCK_SIZE, data, 0);
    } catch (StorageException e) {
      LOG.error("Azure Storage Exception!", e);
      return null;
    }
    List<String> list = null;
    try {
      list = SamzaObjectMapper.getObjectMapper().readValue(data, List.class);
    } catch (IOException e) {
      LOG.error("Failed to read list of live processors from blob", new SamzaException(e));
    }
    return list;
  }

  public CloudBlobClient getBlobClient() {
    return this.blobClient;
  }

  public CloudBlobContainer getBlobContainer() {
    return this.container;
  }

  public CloudPageBlob getBlob() {
    return this.blob;
  }

}