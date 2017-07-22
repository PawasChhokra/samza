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
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BlobUtils {

  private static final Logger LOG = LoggerFactory.getLogger(BlobUtils.class);
  private CloudBlobClient blobClient = null;
  private CloudBlobContainer container = null;
  private CloudPageBlob blob = null;
  private static final long JOB_MODEL_BLOCK_SIZE = 1024000;
  private static final long BARRIER_STATE_BLOCK_SIZE = 1024;
  private static final long PROCESSOR_LIST_BLOCK_SIZE = 1024;

  public BlobUtils(AzureClient client, String containerName, String blobName, long length) {
    this.blobClient = client.getBlobClient();
    try {
      this.container = blobClient.getContainerReference(containerName);
      container.createIfNotExists();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    } catch (StorageException e) {
      e.printStackTrace();
    }
    try {
      this.blob = container.getPageBlobReference(blobName);
      if (!blob.exists()) {
        blob.create(length);
      }
    } catch (URISyntaxException e) {
      e.printStackTrace();
    } catch (StorageException e) {
      e.printStackTrace();
    }
  }


  public void publishJobModel(JobModel prevJM, JobModel currJM, String prevJMV, String currJMV, String leaseId) {
    try {
      JobModelBundle bundle = new JobModelBundle(prevJM, currJM, prevJMV, currJMV);
      ObjectMapper mmapper = SamzaObjectMapper.getObjectMapper();
      byte[] data = mmapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(bundle);
      System.out.println(data.length);
      byte[] pageData = Arrays.copyOf(data, (int) JOB_MODEL_BLOCK_SIZE);
      System.out.println(pageData.length);
      InputStream is = new ByteArrayInputStream(pageData);
      blob.uploadPages(is, 0, JOB_MODEL_BLOCK_SIZE, AccessCondition.generateLeaseCondition(leaseId), null, null);
      LOG.info("Uploaded jobModel to blob");

    } catch (Exception e) {
      LOG.error("JobModel publish failed for version=" + currJMV, e);
      throw new SamzaException(e);
    }
  }


  public JobModel getJobModel() {
    LOG.info("Read the job model from blob.");
    byte[] data = new byte[(int) JOB_MODEL_BLOCK_SIZE];
    try {
      blob.downloadRangeToByteArray(0, JOB_MODEL_BLOCK_SIZE, data, 0);
    } catch (StorageException e) {
      e.printStackTrace();
    }
    ObjectMapper mmapper = SamzaObjectMapper.getObjectMapper();
    JobModelBundle jmBundle;
    JobModel currJM = null;
    try {
      jmBundle = mmapper.readValue(data, JobModelBundle.class);
      currJM = jmBundle.getCurrJobModel();
    } catch (IOException e) {
      throw new SamzaException("failed to read JobModel from blob", e);
    }
    return currJM;
  }

  public String getJobModelVersion() {
    LOG.info("Read the job model version from blob.");
    byte[] data = new byte[(int) JOB_MODEL_BLOCK_SIZE];
    try {
      blob.downloadRangeToByteArray(0, JOB_MODEL_BLOCK_SIZE, data, 0);
    } catch (StorageException e) {
      e.printStackTrace();
    }
    ObjectMapper mmapper = SamzaObjectMapper.getObjectMapper();
    JobModelBundle jmBundle;
    String jmv = null;
    try {
      jmBundle = mmapper.readValue(data, JobModelBundle.class);
      jmv = jmBundle.getCurrJobModelVersion();
    } catch (IOException e) {
      throw new SamzaException("failed to read JobModel version from blob", e);
    }
    return jmv;
  }

  public void publishBarrierState(String state, String leaseId) {
    try {
      ObjectMapper mmapper = SamzaObjectMapper.getObjectMapper();
      byte[] data = mmapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(state);
      System.out.println(data.length);
      byte[] pageData = Arrays.copyOf(data, (int) BARRIER_STATE_BLOCK_SIZE);
      System.out.println(pageData.length);
      InputStream is = new ByteArrayInputStream(pageData);
      blob.uploadPages(is, JOB_MODEL_BLOCK_SIZE, BARRIER_STATE_BLOCK_SIZE, AccessCondition.generateLeaseCondition(leaseId), null, null);
      LOG.info("Uploaded barrier state to blob");

    } catch (Exception e) {
      LOG.error("Barrier state publish failed", e);
      throw new SamzaException(e);
    }
  }

  public String getBarrierState() {
    LOG.info("Read the barrier state from blob.");
    byte[] data = new byte[(int) BARRIER_STATE_BLOCK_SIZE];
    try {
      blob.downloadRangeToByteArray(JOB_MODEL_BLOCK_SIZE, BARRIER_STATE_BLOCK_SIZE, data, 0);
    } catch (StorageException e) {
      e.printStackTrace();
    }
    ObjectMapper mmapper = SamzaObjectMapper.getObjectMapper();
    String state = null;
    try {
      state = mmapper.readValue(data, String.class);
    } catch (IOException e) {
      throw new SamzaException("failed to read barrier state from blob", e);
    }
    return state;
  }

  public void publishLiveProcessorList(List<String> processors, String leaseId) {
    try {
      ObjectMapper mmapper = SamzaObjectMapper.getObjectMapper();
      byte[] data = mmapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(processors);
      System.out.println(data.length);
      byte[] pageData = Arrays.copyOf(data, (int) BARRIER_STATE_BLOCK_SIZE);
      System.out.println(pageData.length);
      InputStream is = new ByteArrayInputStream(pageData);
      blob.uploadPages(is, JOB_MODEL_BLOCK_SIZE + BARRIER_STATE_BLOCK_SIZE, PROCESSOR_LIST_BLOCK_SIZE, AccessCondition.generateLeaseCondition(leaseId), null, null);
      LOG.info("Uploaded barrier state to blob");

    } catch (Exception e) {
      LOG.error("Barrier state publish failed", e);
      throw new SamzaException(e);
    }
  }

  public List<String> getLiveProcessorList() {
    LOG.info("Read the the list of live processors from blob.");
    byte[] data = new byte[(int) PROCESSOR_LIST_BLOCK_SIZE];
    try {
      blob.downloadRangeToByteArray(JOB_MODEL_BLOCK_SIZE + BARRIER_STATE_BLOCK_SIZE, PROCESSOR_LIST_BLOCK_SIZE, data, 0);
    } catch (StorageException e) {
      e.printStackTrace();
    }
    ObjectMapper mmapper = SamzaObjectMapper.getObjectMapper();
    List<String> list = null;
    try {
      list = mmapper.readValue(data, List.class);
    } catch (IOException e) {
      throw new SamzaException("failed to read barrier state from blob", e);
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

  public String getETag() {
    return blob.getProperties().getEtag();
  }

}