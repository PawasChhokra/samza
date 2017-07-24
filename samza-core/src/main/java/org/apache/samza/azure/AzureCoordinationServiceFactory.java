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

import org.apache.samza.config.Config;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.CoordinationServiceFactory;
import org.apache.samza.coordinator.CoordinationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AzureCoordinationServiceFactory implements CoordinationServiceFactory {

  @Override
  public CoordinationUtils getCoordinationService(String groupId, String participantId, Config config) {
    String storageConnectionString = "DefaultEndpointsProtocol=https;" + "AccountName=samzaonazure;"
        + "AccountKey=CTykRMBO0xCpyHXQNf02POGNnjcWyPVYkkX+VFmSLGKVI458a8SpqXldzD7YeGtJs415zdx3GIJasI/hLP8ccA==";
    AzureClient client = new AzureClient(storageConnectionString);

    BlobUtils blob = new BlobUtils(client, "testlease", "testblob", 5120000);
    return new AzureCoordinationUtils(client, blob, new LeaseBlobManager(blob.getBlobContainer(), blob.getBlob()));
  }
}
