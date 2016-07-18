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
 * A bolt that counts the words that it receives
 */
public class StateFilterBolt extends BaseRichBolt
{
  // To output tuples from this bolt to the next stage bolts, if any
  private OutputCollector collector;

  // Map to store the count of the words
  private HashMap<String, String> countMap;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {

    // save the collector for emitting tuples
    collector = outputCollector;

    try{
      // create and initialize the country map
      countMap = new HashMap<String, String>();
      BufferedReader in = new BufferedReader(new FileReader("/root/twitterProject/src/main/java/resources/states.txt"));
      String line = "";
      while ((line = in.readLine()) != null) {
        String parts[] = line.split(",");
        countMap.put(parts[0], parts[1]);
      }
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

    String place;
    String state;

    // get the word from the 1st column of incoming tuple
    String tweet = tuple.getStringByField("tweet");
    String country = tuple.getStringByField("state");

    if (country.indexOf(", ")>-1){
      String[] str = country.split(", ");
      place = str[0];
      state = str[1];

      if (countMap.containsValue(state)){
        // emit the word and count
        collector.emit(new Values(tweet,state));
      }
      else{
        if (state.equals("USA") && countMap.get(place) != null){
          state = countMap.get(place);
          // emit the word and count
          collector.emit(new Values(tweet,state));
        }
      }
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a two columns called 'word' and 'count'

    // declare the first column 'word', second column 'count'
    outputFieldsDeclarer.declare(new Fields("tweet","state"));
  }
}
