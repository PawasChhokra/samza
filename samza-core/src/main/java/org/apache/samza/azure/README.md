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


Steps to run MyTestExampleApp: <br />
1. Create a Kafka topic named "myTest" and feed integers to the stream. <br />
2. Make a config file with the following properties set as follows:  <br />
  -> job.coordinator.factory = org.apache.samza.azure.AzureJobCoordinatorFactory <br /> 
  -> app.class = org.apache.samza.test.MyTestExample <br />
  -> streams.myTest.samza.offset.default = oldest <br />
3. Run MyTestExample app with the following 2 arguments: <br />
  -> --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory <br />
  -> --config-path=file:/<path to your config file> <br />
4. This test application prints the integers in the topic myTest. It shuts down on consuming the integer 10.  
