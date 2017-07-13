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


/**
 * Copyright Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.samza.azure;

import java.io.*;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;
import java.net.URISyntaxException;
import java.rmi.dgc.Lease;
import java.security.InvalidKeyException;
import java.util.Random;


public class TestBlob {

  /**
   * Creates the storage blob client.
   * @param storageConnectionString Azure connection string to connect to storage services.
   * @return The storage blob client.
   * @throws URISyntaxException
   * @throws InvalidKeyException
   */
  public static CloudBlobClient getBlobClient(String storageConnectionString) throws URISyntaxException, InvalidKeyException {
    CloudStorageAccount account;
    try {
      account = CloudStorageAccount.parse(storageConnectionString);
    } catch (IllegalArgumentException|URISyntaxException e) {
      System.out.println("\nConnection string specifies an invalid URI.");
      System.out.println("Please confirm the connection string is in the Azure connection string format.");
      throw e;
    } catch (InvalidKeyException e) {
      System.out.println("\nConnection string specifies an invalid key.");
      System.out.println("Please confirm the AccountName and AccountKey in the connection string are valid.");
      throw e;
    }
    return account.createCloudBlobClient();
  }

  public static void main(String[] args) throws URISyntaxException, InvalidKeyException {
    String storageConnectionString = "DefaultEndpointsProtocol=http;" + "AccountName=samzaonazure;"
        + "AccountKey=CTykRMBO0xCpyHXQNf02POGNnjcWyPVYkkX+VFmSLGKVI458a8SpqXldzD7YeGtJs415zdx3GIJasI/hLP8ccA==";

    CloudBlobClient serviceClient = getBlobClient(storageConnectionString);

    String leaseId = "001";
    LeaseBlobManager leaseBlobManager = new LeaseBlobManager(serviceClient, "testlease", "testblob");
    String lease = leaseBlobManager.acquireLease(60, leaseId, 20000000);
    try {
      Thread.sleep(20000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    boolean status = leaseBlobManager.renewLease(leaseId);
    status = leaseBlobManager.releaseLease(leaseId);

//    try {
//      // Container name must be lower case.
//      CloudBlobContainer container = serviceClient.getContainerReference("myimages");
//      container.createIfNotExists();
//
//      // Upload an image file.
//      CloudBlockBlob blob = container.getBlockBlobReference("puppy.jpeg");
//      File sourceFile = new File("/Users/pchhokra/Downloads/puppy.jpeg");
//      blob.upload(new FileInputStream(sourceFile), sourceFile.length());
//
//      // Download the image file.
//      File destinationFile = new File(sourceFile.getParentFile(), "puppyDownload.jpeg");
//      blob.downloadToFile(destinationFile.getAbsolutePath());
//    } catch (FileNotFoundException fileNotFoundException) {
//      System.out.print("FileNotFoundException encountered: ");
//      System.out.println(fileNotFoundException.getMessage());
//      System.exit(-1);
//    } catch (StorageException storageException) {
//      System.out.print("StorageException encountered: ");
//      System.out.println(storageException.getMessage());
//      System.exit(-1);
//    } catch (Exception e) {
//      System.out.print("Exception encountered: ");
//      System.out.println(e.getMessage());
//      System.exit(-1);
//    }

  }

}
