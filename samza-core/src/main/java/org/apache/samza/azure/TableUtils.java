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
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableQuery;
import java.net.URISyntaxException;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TableUtils.class);
  private CloudTableClient tableClient;
  private CloudTable table;
  //add final
  private static final String PARTITION_KEY = "PartitionKey";
  private static final String ROW_KEY = "RowKey";
  private static final String TIMESTAMP = "Timestamp";

  public TableUtils(AzureClient client, String tableName) {
    this.tableClient = client.getTableClient();
    try {
      this.table = tableClient.getTableReference(tableName);
      table.createIfNotExists();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    } catch (StorageException e) {
      e.printStackTrace();
    }
  }

  public void addProcessorEntity(String jmVersion, String pid, int liveness, boolean isLeader) {
    ProcessorEntity entity = new ProcessorEntity(jmVersion, pid);
    entity.setIsLeader(isLeader);
    entity.setLiveness(liveness);
    TableOperation add = TableOperation.insert(entity);
    try {
      table.execute(add);
    } catch (StorageException e) {
      e.printStackTrace();
    }
  }

  public ProcessorEntity getEntity(String jmVersion, String pid) {
    try {
      TableOperation retrieveEntity = TableOperation.retrieve(jmVersion, pid, ProcessorEntity.class);
      ProcessorEntity entity = table.execute(retrieveEntity).getResultAsType();
      return entity;
    } catch (StorageException e) {
      e.printStackTrace();
    }
    return null;
  }

  public void updateHeartbeat(String jmVersion, String pid) {
    try {
      Random rand = new Random();
      int value = rand.nextInt(10000);
      TableOperation retrieveEntity = TableOperation.retrieve(jmVersion, pid, ProcessorEntity.class);
      ProcessorEntity entity = table.execute(retrieveEntity).getResultAsType();
      entity.setLiveness(value);
      TableOperation update = TableOperation.replace(entity);
      table.execute(update);

    } catch (StorageException e) {
      e.printStackTrace();
    }
  }

  public void updateIsLeader(String jmVersion, String pid, boolean isLeader) {
    try {
      TableOperation retrieveEntity = TableOperation.retrieve(jmVersion, pid, ProcessorEntity.class);
      ProcessorEntity entity = table.execute(retrieveEntity).getResultAsType();
      entity.setIsLeader(isLeader);
      TableOperation update = TableOperation.replace(entity);
      table.execute(update);

    } catch (StorageException e) {
      e.printStackTrace();
    }
  }

  public void deleteProcessorEntity(String jmVersion, String pid) {
    try {
      TableOperation retrieveEntity = TableOperation.retrieve(jmVersion, pid, ProcessorEntity.class);
      ProcessorEntity entity = table.execute(retrieveEntity).getResultAsType();
      TableOperation remove = TableOperation.delete(entity);
      table.execute(remove);
    } catch (StorageException e) {
      e.printStackTrace();
    }
  }


  public Iterable<ProcessorEntity> getEntitiesWithPartition(String partitionKey) {
    // Create a filter condition where the partition key is "Smith".
    String partitionFilter = TableQuery.generateFilterCondition(this.PARTITION_KEY, TableQuery.QueryComparisons.EQUAL, partitionKey);

    // Specify a partition query, using "Smith" as the partition key filter.
    TableQuery<ProcessorEntity> partitionQuery = TableQuery.from(ProcessorEntity.class).where(partitionFilter);

    return table.execute(partitionQuery);

  }

  public CloudTable getTable() {
    return table;
  }

//  public void retrieveColumn() {
//    // Define a projection query that retrieves only the Email property
//    TableQuery<ProcessorEntity> projectionQuery = TableQuery.from(ProcessorEntity.class).select(new String[] {"isLeader"});
//
//    // Define a Entity resolver to project the entity to the Email value.
//    EntityResolver<Boolean> emailResolver = new EntityResolver<Boolean>() {
//      @Override
//      public Boolean resolve(String partitionKey, String rowKey, Date timeStamp, HashMap<String, EntityProperty> properties, String etag) {
//        return properties.get("isLeader").getValueAsBoolean();
//      }
//    };
//
//    // Loop through the results, displaying the Email values.
//    for (boolean projectedString : table.execute(projectionQuery, emailResolver)) {
//      System.out.println(projectedString);
//    }
//
//  }


}