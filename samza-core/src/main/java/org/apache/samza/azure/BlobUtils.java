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

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudPageBlob;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.List;
import org.apache.samza.job.model.JobModel;


public class BlobUtils {

  private CloudBlobClient blobClient = null;
  private CloudBlobContainer container = null;
  private CloudPageBlob leaseBlob = null;

  public BlobUtils(String storageConnectionString, String containerName, String blobName) {
    this.blobClient = createBlobClient(storageConnectionString);
    try {
      this.container = blobClient.getContainerReference(containerName);
      container.createIfNotExists();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    } catch (StorageException e) {
      e.printStackTrace();
    }
    try {
      this.leaseBlob = container.getPageBlobReference(blobName);
    } catch (URISyntaxException e) {
      e.printStackTrace();
    } catch (StorageException e) {
      e.printStackTrace();
    }
  }

  /**
   * Creates the storage blob client.
   * @param storageConnectionString Azure connection string to connect to storage services.
   * @return The storage blob client.
   */
  public static CloudBlobClient createBlobClient(String storageConnectionString) {
    CloudStorageAccount account = null;
    try {
      account = CloudStorageAccount.parse(storageConnectionString);
    } catch (IllegalArgumentException | URISyntaxException e) {
      System.out.println("\nConnection string specifies an invalid URI.");
      System.out.println("Please confirm the connection string is in the Azure connection string format.");
    } catch (InvalidKeyException e) {
      System.out.println("\nConnection string specifies an invalid key.");
      System.out.println("Please confirm the AccountName and AccountKey in the connection string are valid.");
    }
    return account.createCloudBlobClient();
  }

  /**
   * Add processor to the list of active processors if it isn't added already
   */
  public void registerProcessor() {

  }

  /**
   * Delete processor from list of processors
   */
  public void deleteProcessor() {

  }

  /**
   *
   * @return List of active processors in the application
   */
  public List<String> getActiveProcessorsList() {
    return null;
  }

  public void publishJobModel(String jobModelVersion, JobModel jobModel) {

  }

  public JobModel getJobModel(String jobModelVersion) {
    return null;
  }

  public String getJobModelVersion() {
    return null;
  }

  public void publishJobModelVersion(String oldVersion, String newVersion) {

  }

  public CloudBlobClient getBlobClient() {
    return this.blobClient;
  }

  public CloudBlobContainer getBlobContainer() {
    return this.container;
  }

  public CloudPageBlob getLeaseBlob() {
    return this.leaseBlob;
  }

}