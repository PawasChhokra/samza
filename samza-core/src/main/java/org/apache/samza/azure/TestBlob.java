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

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;


public class TestBlob {

  public static void main(String[] args) throws URISyntaxException, InvalidKeyException, StorageException, IOException {

    String storageConnectionString = "DefaultEndpointsProtocol=https;" + "AccountName=samzaonazure;"
        + "AccountKey=CTykRMBO0xCpyHXQNf02POGNnjcWyPVYkkX+VFmSLGKVI458a8SpqXldzD7YeGtJs415zdx3GIJasI/hLP8ccA==";

    BlobUtils blobUtils = new BlobUtils(storageConnectionString, "testlease", "testblob");
    CloudBlobClient serviceClient = blobUtils.createBlobClient(storageConnectionString);

    File tempFile1 = DataGenerator.createTempLocalFile("pageblob1-", ".tmp", 128 * 1024);
    File tempFile2 = DataGenerator.createTempLocalFile("pageblob2-", ".tmp", 128 * 1024);

    String leaseId = null;
    CloudBlobContainer container = blobUtils.getBlobContainer();
    CloudPageBlob leaseBlob = blobUtils.getLeaseBlob();
    LeaseBlobManager leaseBlobManager = new LeaseBlobManager(container, leaseBlob);
    String lease = leaseBlobManager.acquireLease(20, leaseId, 5120000);
    if (lease != null) {
      System.out.println("Acquired lease");
    }
    FileInputStream tempFileInputStream = null;
//    try {
//      tempFileInputStream = new FileInputStream(tempFile1);
//      System.out.println("\t\t\tUploading range start: 0, length: 1024.");
//      leaseBlob.upload(tempFileInputStream, 128 * 1024);
//    }
//    catch (Throwable t) {
//      throw t;
//    }
//    finally {
//      if (tempFileInputStream != null) {
//        tempFileInputStream.close();
//      }
//    }
//    System.out.println("Uploaded 1");
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    boolean status = leaseBlobManager.renewLease(lease);
    if (status) {
      System.out.println("Renewed lease");
      try {
        tempFileInputStream = new FileInputStream(tempFile2);
        System.out.println("\t\t\tUploading range start: 4096, length: 1536.");
        leaseBlob.upload(tempFileInputStream, 128 * 1024);
      } catch (Throwable t) {
        throw t;
      } finally {
        if (tempFileInputStream != null) {
          tempFileInputStream.close();
        }
      }
      System.out.println("Uploaded 2");
    }
    status = leaseBlobManager.releaseLease(lease);
    System.out.println("Released lease = " + status);
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
