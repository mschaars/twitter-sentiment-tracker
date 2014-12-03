import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;

/**
 * Created by Michael on 18.11.2014.
 */
public class Bootstrap {

    static final String TOPOLOGY_NAME = "sentiment-tracker";
    private static final AccessToken accessToken = new AccessToken("2881734113-OIqRfzJt9oj904mXRCKoo7sVgbhCcjRnO4Rvl6g",
            "r1nJBNgPitqKoVtBVBfBwWUIDnWGgMbgwYRmOpinBVnVr");
    private static final String oAuthConsumer = "VyGyG0F3wWk2c6LMzPIUh7BSR";
    private static final String oAuthSecret = "yuR1EV2yrthnvZMofqPChTgRckGtte3xjGicaatIpMem6VdT8g";

    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(120);

        String collection ="test";
        String[] topics = {"#Rokerthon", "#usa", "#obama"};
        String[] languages = {"en"};
        double[][] locations = new double[][] {
                {-124.6,18.9},{-67.0,53.6},
                //{-74.2591,40.496}, {-73.7003,40.9153}
               // {-125.7,28.9},{-67.0,55.5},
        };

        authToTwitterStream();
        TopologyBuilder b = new TopologyBuilder();
        b.setSpout("TwitterSpout", new TwitterStreamSpout(null, locations, languages));
        b.setBolt("FilterBolt", new FilterBolt()).shuffleGrouping("TwitterSpout");
        b.setBolt("TrackerBolt", new TrackerBolt(topics, collection)).shuffleGrouping("FilterBolt");

        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                cluster.killTopology(TOPOLOGY_NAME);
                cluster.shutdown();
            }
        });
    }

    private static void authToTwitterStream() {
        TwitterStream twitterStream = TwitterStreamFactory.getSingleton();
        twitterStream.setOAuthConsumer(oAuthConsumer, oAuthSecret);
        twitterStream.setOAuthAccessToken(accessToken);
    }
}
