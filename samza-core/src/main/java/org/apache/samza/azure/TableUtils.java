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
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.samza.SamzaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  Client side class that has a reference to Azure Table Storage.
 *  Enables the user to make updates to the table and retrieve information from the table.
 */
public class TableUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TableUtils.class);
  private static final String PARTITION_KEY = "PartitionKey";
  private static final String ROW_KEY = "RowKey";
  private static final String TIMESTAMP = "Timestamp";
  private static final long CHECK_LIVENESS_DELAY = 30;
  private static final String INITIAL_STATE = "unassigned";
  private CloudTableClient tableClient;
  private CloudTable table;

  public TableUtils(AzureClient client, String tableName) {
    this.tableClient = client.getTableClient();
    try {
      this.table = tableClient.getTableReference(tableName);
      table.createIfNotExists();
    } catch (URISyntaxException e) {
      LOG.error("\nConnection string specifies an invalid URI.", new SamzaException(e));
    } catch (StorageException e) {
      LOG.error("Azure storage exception.", new SamzaException(e));
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
      LOG.error("Azure storage exception.", new SamzaException(e));
    }
  }

  public ProcessorEntity getEntity(String jmVersion, String pid) {
    try {
      TableOperation retrieveEntity = TableOperation.retrieve(jmVersion, pid, ProcessorEntity.class);
      ProcessorEntity entity = table.execute(retrieveEntity).getResultAsType();
      return entity;
    } catch (StorageException e) {
      LOG.error("Azure storage exception.", new SamzaException(e));
    }
    return null;
  }

  public void updateHeartbeat(String jmVersion, String pid) {
    try {
      Random rand = new Random();
      int value = rand.nextInt(10000) + 2;
      TableOperation retrieveEntity = TableOperation.retrieve(jmVersion, pid, ProcessorEntity.class);
      ProcessorEntity entity = table.execute(retrieveEntity).getResultAsType();
      entity.setLiveness(value);
      TableOperation update = TableOperation.replace(entity);
      table.execute(update);
    } catch (StorageException e) {
      LOG.error("Azure storage exception.", new SamzaException(e));
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
      LOG.error("Azure storage exception.", new SamzaException(e));
    }
  }

  public void deleteProcessorEntity(String jmVersion, String pid) {
    try {
      TableOperation retrieveEntity = TableOperation.retrieve(jmVersion, pid, ProcessorEntity.class);
      ProcessorEntity entity = table.execute(retrieveEntity).getResultAsType();
      TableOperation remove = TableOperation.delete(entity);
      table.execute(remove);
    } catch (StorageException e) {
      LOG.error("Azure storage exception.", new SamzaException(e));
    }
  }


  public Iterable<ProcessorEntity> getEntitiesWithPartition(String partitionKey) {
    String partitionFilter = TableQuery.generateFilterCondition(this.PARTITION_KEY, TableQuery.QueryComparisons.EQUAL, partitionKey);
    TableQuery<ProcessorEntity> partitionQuery = TableQuery.from(ProcessorEntity.class).where(partitionFilter);
    return table.execute(partitionQuery);
  }

  /**
   *
   * @return List of ids of currently active processors in the application
   */
  public Set<String> getActiveProcessorsList(AtomicReference<String> currentJMVersion) {
    Iterable<ProcessorEntity> tableList = getEntitiesWithPartition(currentJMVersion.get());
    Set<String> activeProcessorsList = new HashSet<>();
    for (ProcessorEntity entity: tableList) {
      if (System.currentTimeMillis() - entity.getTimestamp().getTime() <= CHECK_LIVENESS_DELAY * 1000) {
        activeProcessorsList.add(entity.getRowKey());
      }
    }

    Iterable<ProcessorEntity> unassignedList = getEntitiesWithPartition(INITIAL_STATE);
    for (ProcessorEntity entity: unassignedList) {
      if (System.currentTimeMillis() - entity.getTimestamp().getTime() <= CHECK_LIVENESS_DELAY * 1000) {
        activeProcessorsList.add(entity.getRowKey());
      }
    }
    return activeProcessorsList;
  }
  public CloudTable getTable() {
    return table;
  }

}