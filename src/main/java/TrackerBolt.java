import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.mongodb.*;
import data.FilterItem;
import data.MongoClient;
import twitter4j.GeoLocation;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Michael on 20.11.2014.
 */
public class TrackerBolt extends BaseRichBolt {

    private static ConcurrentHashMap<String, FilterItem> statistics = new ConcurrentHashMap<>();;
    private static AtomicInteger tweets = new AtomicInteger(0);
    private String[] topics;
    private static DB mongoDB;
    private static DBCollection coll;

    public TrackerBolt(String[] topics) {
        this.topics = topics;
        mongoDB = MongoClient.getDBInstance("localhost", "sentiment", WriteConcern.ACKNOWLEDGED);
        coll = mongoDB.getCollection("sentiment");

       recoverStatistics();
    }

    /**
     * Loads previous statistics on a topic from MongoDB
     * to allow for interrupted tracking sessions.
     */
    private void recoverStatistics() {
        BasicDBObject dbObject = new BasicDBObject("_id", "last_values");
        DBObject result = coll.findOne(dbObject);

        if (result != null) {
            LinkedHashMap<String, Object> values = (LinkedHashMap<String, Object>) result.toMap();
            LinkedHashMap<String, Object> matches = (LinkedHashMap<String, Object>) values.get("matches");
            LinkedHashMap<String, Object> totalCounters = (LinkedHashMap<String, Object>) values.get("total");

            for (Map.Entry<String, Object> entry : matches.entrySet()) {
                int i = (int) entry.getValue();
                int k = (int) totalCounters.get(entry.getKey());
                statistics.put(entry.getKey(), new FilterItem(k, i));
            }

            for (Map.Entry<String, FilterItem> entry : statistics.entrySet()) {
                System.out.println(entry.getKey() + " value = " + entry.getValue().matchCounter);
                System.out.println(entry.getKey() + " value = " + entry.getValue().totalCounter);

            }
        }
        else {
            System.out.println("No previous statistics found");
        }

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        tweets.incrementAndGet();

        GeoLocation location = (GeoLocation) tuple.getValueByField("location");
        boolean match = containsTopics( (String[]) tuple.getValueByField("words"));

        statistics.compute(LocationService.getCountry(location), (k, v) -> {
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
            statistics.compute("none", (k, v) -> {
                if (v == null) {
                    return new data.FilterItem(1, 0);
                }

                v.totalCounter++;
                return v;
            });
        }*/

        if (tweets.intValue() % 500 == 0) {
            printCounts();
            exportCounts();
        }
    }

    /**
     * Checks if an array of words contains at
     * least one topic-related word.
     * @param words
     * @return
     */
    private boolean containsTopics(String[] words) {
        for (String word : words) {
                for (String topic : topics) {
                if (word.equals(topic)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    /**
     * Export current statistics to MongoDB by replacing
     * the document.
     */
    public void exportCounts() {
        Map<Object, Object> matchValues = new HashMap<>();
        Map<Object, Object> totalValues = new HashMap<>();

        for (Map.Entry<String, FilterItem> entry : statistics.entrySet()) {
            FilterItem data = entry.getValue();
            String key = entry.getKey();
            matchValues.put(key, data.matchCounter);
            totalValues.put(key, data.totalCounter);
        }

        BasicDBObject obj = new BasicDBObject("_id", "last_values").append("matches", matchValues).
                append("total", totalValues);

        if (coll.count() == 0) {
            coll.insert(obj);
        }
        else {
            BasicDBObject q = new BasicDBObject("_id","last_values");
            coll.update(q, obj);
        }
    }

    /**
     * Print current statistics to terminal.
     */
    public void printCounts() {
        for (Map.Entry<String, FilterItem> e : statistics.entrySet()) {
            FilterItem filterItem = e.getValue();
            System.out.println("Location = " + e.getKey() + ", tweets = " + filterItem.totalCounter
                    + ", topic statistics = " + filterItem.matchCounter);

        }

        System.out.println("Total tweets seen = " + tweets.toString() +"\n ###############################");
    }

}
