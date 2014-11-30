import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.clearspring.analytics.stream.ConcurrentStreamSummary;
import com.clearspring.analytics.stream.ScoredItem;
import com.google.common.primitives.Ints;
import com.mongodb.*;
import data.FilterItem;
import data.MongoClient;
import twitter4j.GeoLocation;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Main processing class. Scans tweets witin location
 * for topics and keeps track of most relevant keywords (filtering out stop words)
 * using a sketch data structure.
 */
public class TrackerBolt extends BaseRichBolt {

    private static ConcurrentHashMap<String, FilterItem> statistics = new ConcurrentHashMap<>();
    private static ConcurrentStreamSummary<String> heavyHitters = new ConcurrentStreamSummary<>(10);

    private static Set<String> stopWords = new HashSet<String>(Arrays.asList(new String[] {
            "I", ",", "http", "the", "you", "and", "for", "that", "like", "have", "this", "just", "with", "all", "get", "about",
            "can", "was", "not", "your", "but", "are", "one", "what", "out", "when", "get","of", "lol", "now",
            "want", "will", "know", "good", "from","people", "got", "why", "time", "would", "it","can't",
            "me", "to","is"
    }));

    private static AtomicInteger tweets = new AtomicInteger(0);
    private String[] topics;
    private static DB mongoDB;
    private static DBCollection coll;

    public TrackerBolt(String[] topics, String collection) {
        this.topics = topics;
        mongoDB = MongoClient.getDBInstance("localhost", "testing", WriteConcern.ACKNOWLEDGED);
        coll = mongoDB.getCollection(collection);

       recoverStatistics();
    }



    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        tweets.incrementAndGet();

        GeoLocation location = (GeoLocation) tuple.getValueByField("location");
        boolean match = containsTopics( (String[]) tuple.getValueByField("words"));

        statistics.compute(LocationService.getNameForLocation(location), (k, v) -> {
            if (v == null) {
                return match ? new FilterItem(1, 1) : new FilterItem(1, 0);
            }
            v.totalCounter++;
            if (match) {
                v.matchCounter++;
            }
            return v;
        });

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
                    captureSentiment(words);
                    return true;
                }
            }
        }
        return false;
    }

    //TODO investigate calls to this
    private void captureSentiment(String[] words) {
        for (String word : words) {
            if (!stopWords.contains(word)) {
                if (word.contains(".")) {
                    word = word.replace("."," ");
                }
                heavyHitters.offer(word);
            }
        }
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

        List<ScoredItem<String>> topK = heavyHitters.peekWithScores(10);
        Map<Object, Object> hitters = new HashMap<>();

        for (ScoredItem<String> entry : topK) {
            hitters.put(entry.getItem(), entry.getCount());
        }

        BasicDBObject obj = new BasicDBObject("_id", "last_values").append("matches", matchValues).
                append("total", totalValues).append("popular", hitters);

        if (coll.count() == 0) {
            coll.insert(obj);
        }
        else {
            BasicDBObject q = new BasicDBObject("_id","last_values");
            coll.update(q, obj);
        }
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
            LinkedHashMap<String, Object> topK = (LinkedHashMap<String, Object>) values.get("popular");

            for (Map.Entry<String, Object> entry : matches.entrySet()) {
                int i = (int) entry.getValue();
                int k = (int) totalCounters.get(entry.getKey());
                statistics.put(entry.getKey(), new FilterItem(k, i));
            }
            if (topK != null) {
                for (Map.Entry<String, Object> entry : topK.entrySet()) {
                    heavyHitters.offer(entry.getKey(), Ints.checkedCast((long)entry.getValue()));
                }
            }

            System.out.println("Loading previous statistics..");
            for (Map.Entry<String, FilterItem> entry : statistics.entrySet()) {
                System.out.println(entry.getKey() + " matches = " + entry.getValue().matchCounter);
                System.out.println(entry.getKey() + " total = " + entry.getValue().totalCounter);

            }
        }
        else {
            System.out.println("No previous statistics found");
        }
    }

    /**
     * Print current statistics to terminal.
     */
    public void printCounts() {
        List<ScoredItem<String>> scores = heavyHitters.peekWithScores(10);
        for (Map.Entry<String, FilterItem> e : statistics.entrySet()) {
            FilterItem filterItem = e.getValue();
            System.out.println("Location = " + e.getKey() + ", tweets = " + filterItem.totalCounter
                    + ", topic related = " + filterItem.matchCounter);

        }
        System.out.println("Most popular words in relevant tweets:");
        for (ScoredItem<String> item : scores) {
            System.out.println("Word = " + item.getItem() + ", count = " + item.getCount());
        }

        System.out.println("Total tweets this session = " + tweets.toString() +"\n ###############################");
    }


}
