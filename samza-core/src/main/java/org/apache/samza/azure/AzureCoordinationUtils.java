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

import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.Latch;
import org.apache.samza.coordinator.LeaderElector;
import org.apache.samza.coordinator.Lock;


public class AzureCoordinationUtils implements CoordinationUtils {

  private final BlobUtils blob;
  private final AzureClient client;
  private final LeaseBlobManager leaseBlobManager;

  public AzureCoordinationUtils(AzureClient client, BlobUtils blob, LeaseBlobManager leaseBlobManager) {
    this.blob = blob;
    this.client = client;
    this.leaseBlobManager = leaseBlobManager;
  }

  @Override
  public void reset() {

  }

  @Override
  public LeaderElector getLeaderElector() {
    return new AzureLeaderElector(leaseBlobManager);
  }

  @Override
  public Latch getLatch(int size, String latchId) {
    return null;
  }

  @Override
  public Lock getLock() {
    return new AzureLock(blob);
  }
}