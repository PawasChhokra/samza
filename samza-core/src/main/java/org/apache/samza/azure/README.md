Steps to run MyTestExampleApp:
1. Create a Kafka topic named "myTest" and feed integers to the stream.
2. Make a config file with the following properties set as follows:
  -> job.coordinator.factory = org.apache.samza.azure.AzureJobCoordinatorFactory  
  -> app.class = org.apache.samza.test.MyTestExample
  -> streams.myTest.samza.offset.default = oldest
3. Run MyTestExample app with the following 2 arguments: 
  -> --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory
  -> --config-path=file:/<path to your config file>
4. This test application prints the integers in the topic myTest. It shuts down on consuming the integer 10.
