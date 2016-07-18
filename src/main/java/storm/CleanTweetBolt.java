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

import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher; 

/**
 * A bolt that parses the tweet into words
 */
public class CleanTweetBolt extends BaseRichBolt 
{
  // To output tuples from this bolt to the count bolt
  OutputCollector collector;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector) 
  {
    // save the output collector for emitting tuples
    collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) 
  {
    // get the 1st column 'tweet' from tuple
    String tweet = tuple.getStringByField("tweet");
    String state = tuple.getStringByField("state");

    // Delete URLS in tweet
    String urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
    Pattern p = Pattern.compile(urlPattern,Pattern.CASE_INSENSITIVE);
    Matcher m = p.matcher(tweet);
    while (m.find()) tweet = tweet.replaceAll(Pattern.quote(m.group()),"").trim();

    // Delete all no words
    tweet = tweet.replaceAll("[^a-zA-Z0-9\\sˆ@#_]", " ").trim().toLowerCase();
    tweet = tweet.replaceAll("\\s{2,}", " ");

    // Delete all words that have less than 3 characters
    tweet = tweet.replaceAll("\\b[\\w']{1,2}\\b", " ");
    tweet = tweet.replaceAll("\\s{2,}", " ");

    tweet = tweet.replaceAll(" @ ", " ");
    tweet = tweet.replaceAll(" # ", " ");
    tweet = tweet.replaceAll("\\s{2,}", " ");

    // Emit the tuple
    collector.emit(new Values(tweet, state));

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) 
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a single column called 'tweet-word'
    declarer.declare(new Fields("tweet","state"));
  }
}
