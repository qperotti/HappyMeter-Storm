package storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;




import java.text.DecimalFormat;




/**
 * A bolt that prints the word and count to redis
 */
public class RedisStoreBolt extends BaseRichBolt
{
  // place holder to keep the connection to redis
  transient RedisConnection<String,String> redis;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    // instantiate a redis connection
    RedisClient client = new RedisClient("*.*.*.*",6379);

    // initiate the actual connection
    redis = client.connect();
    redis.auth("*******");
  }

  @Override
  public void execute(Tuple tuple)
  {

    DecimalFormat finalscore = new DecimalFormat("#.00");

    // access the first column 'word'
    String tweet = tuple.getStringByField("tweet");
    String state = tuple.getStringByField("state");
    float score = tuple.getFloatByField("score");


    // publish the word count to redis using word as the key
    redis.rpush(state, tweet + "|" + finalscore.format(score));

  }

  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // nothing to add - since it is the final bolt
  }
}
