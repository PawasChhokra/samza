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

import com.microsoft.azure.storage.table.TableServiceEntity;


/**
 * Table schema for Azure processor table.
 * Denotes a row in the table.
 */
public class ProcessorEntity extends TableServiceEntity {
  private int liveness;
  private boolean isLeader;

  public ProcessorEntity() {}

  public ProcessorEntity(String jobModelVersion, String processorId) {
    this.partitionKey = jobModelVersion;
    this.rowKey = processorId;
    this.isLeader = false;
  }

  /**
   * Updates heartbeat by changing the liveness value in the table
   * @param value
   */
  public void setLiveness(int value) {
    liveness = value;
  }

  public void setIsLeader(boolean leader) {
    isLeader = leader;
  }

  public boolean getIsLeader() {
    return isLeader;
  }
}
