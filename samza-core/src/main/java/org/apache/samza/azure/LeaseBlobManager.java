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
import com.microsoft.azure.storage.ServiceProperties;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudPageBlob;
import java.net.URISyntaxException;
import org.eclipse.jetty.http.HttpStatus;


public class LeaseBlobManager {

  private CloudBlobContainer container;
  private CloudPageBlob leaseBlob;

  public LeaseBlobManager(CloudBlobClient blobClient, String containerName, String blobName) {
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
      ServiceProperties temp = leaseBlob.getServiceClient().downloadServiceProperties();
      temp.setDefaultServiceVersion("2016-05-31");
      leaseBlob.getServiceClient().uploadServiceProperties(temp);
      System.out.print("a");
    } catch (URISyntaxException e) {
      e.printStackTrace();
    } catch (StorageException e) {
      e.printStackTrace();
    }
  }

  /**
   * Acquires a lease on a blob.
   * If the blob does not exist, it creates a blob and then tries to acquire a lease on it.
   * @param leaseTimeInSec The time in seconds you want to acquire the lease for.
   * @param leaseId Proposed ID you want to acquire the lease with, null if not proposed.
   * @param length Size in bytes, of the page blob (needed when the blob doesn't exist and needs to be created).
   * @return String that represents lease ID. Null if lease is not acquired.
   */
  public String acquireLease(int leaseTimeInSec, String leaseId, long length) {
    try {
      String id = leaseBlob.acquireLease(leaseTimeInSec, leaseId);
      return id;
    } catch (StorageException storageException) {
        int httpStatusCode = storageException.getHttpStatusCode();

        if (httpStatusCode == HttpStatus.NOT_FOUND_404) {
          createBlob(length);
          acquireLease(leaseTimeInSec, leaseId, length);
        } else {
          return null;
        }
    }
    return null;
  }

  /**
   * Renews the lease on the blob.
   * @param leaseId ID of the lease to be renewed.
   * @return True is lease was renewed successfully, false otherwise.
   */
  public boolean renewLease(String leaseId) {
    try {
      leaseBlob.renewLease(AccessCondition.generateLeaseCondition(leaseId));
      return true;
    } catch (StorageException e) {
      e.printStackTrace();
      //TODO: Trigger leader election
      return false;
    }
  }

  /**
   * Releases the lease on the blob.
   * @param leaseId ID of the lease to be released.
   * @return True if released successfully, false otherwise.
   */
  public boolean releaseLease(String leaseId) {
    try {
      leaseBlob.releaseLease(AccessCondition.generateLeaseCondition(leaseId));
      return true;
    } catch (StorageException e) {
      e.printStackTrace();
      return false;
    }
  }

  private void createBlob(long length) {
    try {
      if (!leaseBlob.exists()) {
        leaseBlob.create(length);
      }

      try {
        leaseBlob.getContainer().createIfNotExists();
      } catch (StorageException e) {
        e.printStackTrace();
      } catch (URISyntaxException e) {
        e.printStackTrace();
      }

    } catch (StorageException e) {
      e.printStackTrace();
    }
  }

}
