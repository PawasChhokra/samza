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
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SystemStreamPartition;


public class TestBlob {

  public static void main(String[] args)
      throws URISyntaxException, InvalidKeyException, StorageException, IOException, ClassNotFoundException {

    String storageConnectionString = "DefaultEndpointsProtocol=https;" + "AccountName=samzaonazure;"
        + "AccountKey=CTykRMBO0xCpyHXQNf02POGNnjcWyPVYkkX+VFmSLGKVI458a8SpqXldzD7YeGtJs415zdx3GIJasI/hLP8ccA==";

    AzureClient client = new AzureClient(storageConnectionString);
    BlobUtils blobUtils = new BlobUtils(client, "testlease", "testblob", 5120000);

    BlobUtils blobUtils2 = new BlobUtils(client, "testlease", "testblob2", 5120000);

//    File tempFile1 = DataGenerator.createTempLocalFile("pageblob1-", ".tmp", 128 * 1024);
//    File tempFile2 = DataGenerator.createTempLocalFile("pageblob2-", ".tmp", 128 * 1024);

    String leaseId = null;
    CloudBlobContainer container = blobUtils.getBlobContainer();


    CloudPageBlob leaseBlob = blobUtils.getBlob();
    JobModel currJobModel = getCurrJobModel();
    JobModel prevJobModel = getPrevJobModel();
    JobModel first = null;
    blobUtils.publishJobModel(null, prevJobModel, null, "1", null);
//    blobUtils.publishJobModel(prevJobModel, currJobModel, "1", "2");
    JobModel returnedJobModel = blobUtils.getJobModel();
    if (prevJobModel.equals(returnedJobModel)) {
      System.out.print("true");
    }

    String jobModelVersion = "1";
    String returnedJMV = blobUtils.getJobModelVersion();
    if (jobModelVersion.equals(returnedJMV)) {
      System.out.print("true");
    }

//    String barrierState = "created_1";
//    blobUtils.updateBarrierState(barrierState);
//    String returnedState = blobUtils.getBarrierState();
//    if (barrierState.equals(returnedState)) {
//      System.out.print("true");
//    }
//
//    Set<String> list = new HashSet<String>();
//    list.add("1");
//    list.add("2");
//    list.add("3");
//    blobUtils.publishLiveProcessorList(list);
//    Set<String> returnedList = blobUtils.getLiveProcessorList();
//    if (list.equals(returnedList)) {
//      System.out.println("true");
//    }

//    LeaseBlobManager leaseBlobManager = new LeaseBlobManager(container, leaseBlob);
//    String lease = leaseBlobManager.acquireLease(20, leaseId, 5120000);
//    if (lease != null) {
//      System.out.println("Acquired lease");
//    }
//    FileInputStream tempFileInputStream = null;
//    try {
//
//      JobModel jobModel = getJobModel();
//
//      ByteArrayOutputStream baos = new ByteArrayOutputStream();
//      ObjectOutputStream oos = new ObjectOutputStream(baos);
//
//      oos.writeObject(jobModel);
//
//      oos.flush();
//      oos.close();
//
//      int length = baos.toByteArray().length + 512 - baos.toByteArray().length%512;
////      /512+1)*512;
//      byte[] temp = Arrays.copyOf(baos.toByteArray(), length);
//      int offset = length - baos.toByteArray().length;
//      InputStream is = new ByteArrayInputStream(temp);
//
////      tempFileInputStream = new FileInputStream(tempFile1);
//      System.out.println("\t\t\tUploading range start: 0, length: 1024.");
//      leaseBlob.upload(is, length);
//
//      byte[] output = new byte[length];
//      leaseBlob.downloadToByteArray(output, 0);
//      output = Arrays.copyOfRange(output, 0, baos.toByteArray().length);
//      ByteArrayInputStream in = new ByteArrayInputStream(output);
//      ObjectInputStream is2 = new ObjectInputStream(in);
//      JobModel j = (JobModel) is2.readObject();
//      System.out.println(jobModel);
//      System.out.println(j);
//      System.out.println("Uploaded 1");
//    }
//    catch (Throwable t) {
//      throw t;
//    }
//    finally {
//      if (tempFileInputStream != null) {
//        tempFileInputStream.close();
//      }
//    }



//    try {
//      Thread.sleep(10000);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
//    boolean status = leaseBlobManager.renewLease(lease);
//    if (status) {
//      System.out.println("Renewed lease");
//      try {
//        tempFileInputStream = new FileInputStream(tempFile2);
//
//
//
//
//        System.out.println("\t\t\tUploading range start: 4096, length: 1536.");
//        leaseBlob.upload(tempFileInputStream, 128 * 1024, AccessCondition.generateLeaseCondition(lease), null, null);
//      } catch (Throwable t) {
//        throw t;
//      } finally {
//        if (tempFileInputStream != null) {
//          tempFileInputStream.close();
//        }
//      }
//      System.out.println("Uploaded 2");
//    }
//    status = leaseBlobManager.releaseLease(lease);
//    System.out.println("Released lease = " + status);


  }

  private static JobModel getPrevJobModel() {
    Map<String, String> configMap = new HashMap<String, String>();
    Set<SystemStreamPartition> ssp = new HashSet<>();
    configMap.put("a", "b");
    Config config = new MapConfig(configMap);
    TaskName taskName = new TaskName("test");
    ssp.add(new SystemStreamPartition("foo", "bar", new Partition(1)));
    TaskModel taskModel = new TaskModel(taskName, ssp, new Partition(2));
    Map<TaskName, TaskModel> tasks = new HashMap<TaskName, TaskModel>();
    tasks.put(taskName, taskModel);
    ContainerModel containerModel = new ContainerModel("1", 1, tasks);
    Map<String, ContainerModel> containerMap = new HashMap<String, ContainerModel>();
    containerMap.put("1", containerModel);
    return new JobModel(config, containerMap);
  }

  private static JobModel getCurrJobModel() {
    Map<String, String> configMap = new HashMap<String, String>();
    Set<SystemStreamPartition> ssp = new HashSet<>();
    configMap.put("c", "d");
    Config config = new MapConfig(configMap);
    TaskName taskName = new TaskName("test");
    ssp.add(new SystemStreamPartition("foo", "bar", new Partition(1)));
    TaskModel taskModel = new TaskModel(taskName, ssp, new Partition(2));
    Map<TaskName, TaskModel> tasks = new HashMap<TaskName, TaskModel>();
    tasks.put(taskName, taskModel);
    ContainerModel containerModel = new ContainerModel("1", 1, tasks);
    Map<String, ContainerModel> containerMap = new HashMap<String, ContainerModel>();
    containerMap.put("1", containerModel);
    return new JobModel(config, containerMap);
  }
}
