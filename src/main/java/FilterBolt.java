import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.GeoLocation;
import twitter4j.Status;

import java.util.Map;

/**
 * Created by Michael on 18.11.2014.
 */
public class FilterBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Status tweet = (Status) tuple.getValueByField("tweet");
        GeoLocation location = tweet.getGeoLocation();

        if (location != null) {
            String[] words = tweet.getText().split(" ");
            outputCollector.emit(new Values(words, location));
        }

       System.out.println("["+ tweet.getCreatedAt().toString() +"]" + tweet.getText());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("words", "location"));

    }
}
