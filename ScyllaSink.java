import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.json.JSONObject;

import java.util.Map;

public class ScyllaSink implements Sink<JSONObject> {
    private static final int NUM_SHARDS = 10;
    private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

    private Cluster cluster;
    private Session session;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        String host = (String) config.get("host");
        String keyspace = (String) config.get("keyspace");
        String table = (String) config.get("table");
        String username = (String) config.get("username");
        String password = (String) config.get("password");

        cluster = Cluster.builder()
                .addContactPoint(host)
                .withCredentials(username, password)
                .build();
        session = cluster.connect(keyspace);

        // Create the Scylla table if it does not already exist
        String createTableCql = String.format("CREATE TABLE IF NOT EXISTS %s (id uuid PRIMARY KEY, data json)", table);
        session.execute(createTableCql);
    }

    @Override
    public void write(Record<JSONObject> record) throws Exception {
        Message<JSONObject> message = record.getValue();
        JSONObject json = message.getValue();

        // Determine the shard key for this message based on its message ID
        String shardKey = getShardKey(message.getMessageId());

        // Write the JSON data to the Scylla table, using the shard key as the partition key
        session.execute(
                QueryBuilder.insertInto(table)
                        .value("id", message.getMessageId().toString())
                        .value("data", json)
                        .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
                        .using(QueryBuilder.ttl(message.getTimeToLive()))
                        .setRoutingKey(shardKey)
        );
    }

    @Override
    public void close() throws Exception {
        session.close();
       

        To build a shard-aware Pulsar connector for Scylla in Java, you will need to first familiarize yourself with the Scylla Java Driver and the Pulsar Java API. Once you have a good understanding of these two libraries, you can begin implementing the connector by following these steps:

Create a new Java project and add the Scylla Java Driver and Pulsar Java API as dependencies.

Define a ScyllaPulsarConnector class that extends the Pulsar Connector class. This class will be responsible for connecting to both Scylla and Pulsar, and for forwarding messages between the two systems.

In the ScyllaPulsarConnector class, implement a constructor that takes in the necessary connection details for both Scylla and Pulsar (e.g. hostname, port, username, password, etc.).

In the constructor, use the Scylla Java Driver to establish a connection to Scylla. You may need to create a Cluster object and a Session object to manage the connection.

Also in the constructor, use the Pulsar Java API to establish a connection to Pulsar. You will need to create a PulsarClient object to manage the connection.

Implement a method in the ScyllaPulsarConnector class that listens for messages on a specified Pulsar topic and forwards them to a specified Scylla table. This method should use the Scylla Java Driver to execute a INSERT statement on the Scylla table, using the data from the Pulsar message as the values to insert.

Test your connector by running a simple producer/consumer test that sends messages to a Pulsar topic and verifies that they are correctly inserted into the Scylla table.

Note that this is just a high-level overview of how to build a shard-aware Pulsar connector for Scylla in Java. There may be additional steps and considerations involved in implementing a production-ready connector, such as handling errors and edge cases, optimizing performance, and so on.
    
    
