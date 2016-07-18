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

class TweetTopology
{
  public static void main(String[] args) throws Exception
  {
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    /*
     * In order to create the spout, you need to get twitter credentials
     * If you need to use Twitter firehose/Tweet stream for your idea,
     * create a set of credentials by following the instructions at
     *
     * https://dev.twitter.com/discussions/631
     *
     */

    // now create the tweet spout with the credentials
    TweetSpout tweetSpout = new TweetSpout(
        "****",
        "****",
        "****",
        "****"
    );

    //*********************************************************************
    // Complete the Topology.
    // attach the tweet spout to the topology - parallelism of 1
    builder.setSpout("tweet-spout", tweetSpout, 1);

    // attach the lean-tweet-bolt using shuffle grouping
    builder.setBolt("clean-tweet-bolt", new CleanTweetBolt(), 5).shuffleGrouping("tweet-spout");

    // attach the stopwords-bolt using shuffle grouping
    builder.setBolt("stopwords-bolt", new StopWordsBolt(), 5).shuffleGrouping("clean-tweet-bolt");

    // attach the state-filter-bolt using shuffle grouping
    builder.setBolt("state-filter-bolt", new StateFilterBolt(), 5).shuffleGrouping("stopwords-bolt");


    // attach the score-bolt using shuffle grouping
    builder.setBolt("score-bolt", new ScoreBolt(), 5).shuffleGrouping("state-filter-bolt");

    // attach the redis-bolt using huffle grouping
    builder.setBolt("redis-bolt", new RedisStoreBolt(), 1).shuffleGrouping("score-bolt");


    //*********************************************************************

    // create the default config object
    Config conf = new Config();

    // set the config in debugging mode
    conf.setDebug(true);

    if (args != null && args.length > 0) {

      // run it in a live cluster

      // set the number of workers for running all spout and bolt tasks
      conf.setNumWorkers(1);

      // create the topology and submit with config
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

    } else {

      // run it in a simulated local cluster

      // set the number of threads to run - similar to setting number of workers in live cluster
      conf.setMaxTaskParallelism(3);

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster();

      // submit the topology to the local cluster
      cluster.submitTopology("tweet-word-count", conf, builder.createTopology());

      // let the topology run for 300 seconds. note topologies never terminate!
      Utils.sleep(300000);

      // now kill the topology
      cluster.killTopology("tweet-word-count");

      // we are done, so shutdown the local cluster
      cluster.shutdown();
    }
  }
}