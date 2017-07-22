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

import com.microsoft.azure.storage.StorageException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public class TestTable {

  public static void main(String[] args) throws URISyntaxException, InvalidKeyException, StorageException, IOException {

    String storageConnectionString = "DefaultEndpointsProtocol=https;" + "AccountName=samzaonazure;"
        + "AccountKey=CTykRMBO0xCpyHXQNf02POGNnjcWyPVYkkX+VFmSLGKVI458a8SpqXldzD7YeGtJs415zdx3GIJasI/hLP8ccA==";

    AzureClient client = new AzureClient(storageConnectionString);

    // Create the table if it doesn't exist.
    String tableName = "testtable";
    TableUtils table = new TableUtils(client, tableName);
//    table.addProcessorEntity("1", "11", 1, true);
//    ProcessorEntity entity = table.getEntity("1", "11");
//    table.updateHeartbeat("1", "11");
//    table.updateIsLeader("1", "11", false);
//    table.addProcessorEntity("1", "22", 1, false);
//    table.addProcessorEntity("2", "11", 1, true);
//    table.addProcessorEntity("1", "33", 1, true);
//    Iterable<ProcessorEntity> tableList = table.getEntitiesWithPartition("1");
//    Set<String> activeProcessorsList = new HashSet<>();
//    for (ProcessorEntity entity: tableList) {
//      activeProcessorsList.add(entity.getRowKey());
//    }
//    table.deleteProcessorEntity("1", "33");

  }

}
