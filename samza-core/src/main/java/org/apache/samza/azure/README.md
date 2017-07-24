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
