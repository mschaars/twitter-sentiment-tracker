import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import twitter4j.GeoLocation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Michael on 20.11.2014.
 */
public class TrackerBolt extends BaseRichBolt {

    private static ConcurrentHashMap<String, FilterItem> matches = new ConcurrentHashMap<>();;
    private static AtomicInteger tweets = new AtomicInteger(0);
    private String[] topics;

    public TrackerBolt(String[] topics) {
        this.topics = topics;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        tweets.incrementAndGet();

        GeoLocation location = (GeoLocation) tuple.getValueByField("location");
        boolean match = containsTopics( (String[]) tuple.getValueByField("words"));

        matches.compute(LocationService.getCountry(location), (k, v) -> {
            if (v == null) {
                return match ? new FilterItem(1, 1) : new FilterItem(1, 0);
            }
            v.totalCounter++;
            if (match) {
                v.matchCounter++;
            }
            return v;
        });

/*        else {
            matches.compute("none", (k, v) -> {
                if (v == null) {
                    return new FilterItem(1, 0);
                }

                v.totalCounter++;
                return v;
            });
        }*/

        if (tweets.intValue() % 5000 == 0) {
            exportCounts();
        }
    }

    private boolean containsTopics(String[] words) {
        for (String word : words) {
                for (String topic : topics) {
                if (word.equals(topic)) {
                    System.out.println("found wordmatch = " + word);
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void exportCounts() {



        for (Map.Entry<String, FilterItem> e : matches.entrySet()) {
            FilterItem filterItem = e.getValue();
            System.out.println("Location = " + e.getKey() + ", tweets = " + filterItem.totalCounter
                    + ", topic matches = " + filterItem.matchCounter);

        }

        System.out.println("Total tweets seen = " + tweets.toString() +"\n ###############################");
    }

    public int countTopicMatches() {
        int s = 0;

        for (Map.Entry<String, FilterItem> e : matches.entrySet()) {
            s += e.getValue().matchCounter;
        }
            return s;
    }
}
           /* System.out.println("location = "  + LocationService.getNameForLocation(location));
            System.out.println("tweet = " + tweet.getText());*/