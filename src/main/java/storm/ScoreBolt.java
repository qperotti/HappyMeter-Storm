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




/**
 * A bolt that prints the word and count to redis
 */
public class ScoreBolt extends BaseRichBolt
{

  // To output tuples from this bolt to the next stage bolts, if any
  private OutputCollector collector;

  // Map to store the count of the words
  private HashMap<String, Float> sentimentalScore;


  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {

  

    collector = outputCollector;
    try{
      // create and initialize the country map
      sentimentalScore = new HashMap<String, Float>();
      BufferedReader in = new BufferedReader(new FileReader("/root/twitterProject/src/main/java/resources/labMit.txt"));
      String line = "";
      while ((line = in.readLine()) != null) sentimentalScore.put(line.split("\t")[0], Float.parseFloat(line.split("\t")[2]));
      in.close();
    }
    catch(IOException ioe){
      //Your error Message here
      System.out.println(ioe);
    }
  }

  @Override
  public void execute(Tuple tuple)
  {

    // access the first column 'word'
    String tweet = tuple.getStringByField("tweet");
    String state = tuple.getStringByField("state");
    
    String[] words = tweet.split(" ");
    float numWords = 0;
    float score = 0;

    for (String word : words){
        if (sentimentalScore.get(word) != null){
          numWords+=1;
          score +=sentimentalScore.get(word);
        }
    }

    score = score/numWords;

    if (score > 0) {
      collector.emit(new Values(tweet,state,score));
    }
  }


  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a two columns called 'word' and 'count'

    // declare the first column 'word', second column 'count'
    outputFieldsDeclarer.declare(new Fields("tweet","state","score"));
  }

}
