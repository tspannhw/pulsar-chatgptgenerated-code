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
       
