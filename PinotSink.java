/**
A Pulsar Sink is a type of connector that allows you to stream data from a Pulsar topic to another destination, such as Apache Pinot. Here is an example of a Pulsar Sink that streams data from a Pulsar topic to Apache Pinot:

In this example, the PinotSink class implements the Sink interface from the Pulsar API. 
The open method is called when the sink is started, and it is used to create a new PinotClient instance and connect to the Apache Pinot cluster. 
The write method is called whenever a new message is received on the Pulsar topic, and it is used to convert the message to a string, 
parse it into a map of key-value pairs, and send the data to Apache Pinot using the PinotClient. Finally, 
the close method is called when the sink is stopped, and it is used to close the PinotClient instance.

*/
public class PinotSink implements Sink<byte[]> {

  // Apache Pinot client instance
  private PinotClient pinotClient;

  @Override
  public void open(Map<String, Object> conf, SinkContext sinkContext) {
    // Create a new Pinot client instance and connect to the Pinot cluster
    String pinotClusterUrl = (String) conf.get("pinotClusterUrl");
    pinotClient = new PinotClient(pinotClusterUrl);
  }

  @Override
  public void write(byte[] data) {
    // Convert the byte array to a string
    String dataString = new String(data, StandardCharsets.UTF_8);

    // Parse the string into a map of key-value pairs
    Map<String, Object> dataMap = parseData(dataString);

    // Send the data to Apache Pinot using the Pinot client
    pinotClient.send(dataMap);
  }

  @Override
  public void close() {
    // Close the Pinot client instance
    pinotClient.close();
  }
}
