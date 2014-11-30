import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Basic spout for streaming Ebola-related tweets.
 *
 */
public class TwitterStreamSpout extends BaseRichSpout {

    private final String[] topics;

    private final String[] languages;
    private final double[][] locations;
    private SpoutOutputCollector spoutOutputCollector;
    private LinkedBlockingQueue<Status> incomingQueue;

    public TwitterStreamSpout(String[] topics, double[][] locations, String[] languages) {
        this.topics = topics;
        this.locations = locations;
        this.languages = languages;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        incomingQueue = new LinkedBlockingQueue<>(1000);
        this.spoutOutputCollector = spoutOutputCollector;

        StatusListener listener = new StatusListener() {

            @Override
            public void onStatus(Status status) {
                incomingQueue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            @Override
            public void onTrackLimitationNotice(int i) {

            }

            @Override
            public void onScrubGeo(long l, long l2) {

            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {

            }

            @Override
            public void onException(Exception e) {

            }
        };

        TwitterStream twitterStream =  TwitterStreamFactory.getSingleton();
        twitterStream.addListener(listener);

        FilterQuery filterQuery = new FilterQuery();
        filterQuery = filterQuery.language(languages).locations(locations).track(topics);
        twitterStream.filter(filterQuery);
    }

    @Override
    public void nextTuple() {
        Status ret = incomingQueue.poll();
        //TODO how to simulate this from historical data?
        if (ret == null) {
            Utils.sleep(25);
        } else {
            spoutOutputCollector.emit(new Values(ret));
        }
    }
}
