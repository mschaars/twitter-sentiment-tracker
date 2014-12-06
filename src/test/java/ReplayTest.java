import data.ReplayService;
import org.junit.Test;

import java.util.HashSet;

/**
 * Used to test properties of
 * historical data.
 */
public class ReplayTest {

    @Test
    public void countUniqueWords() {
        ReplayService.readTweets("historic.csv");

        HashSet<String> uniques = new HashSet<>();
        String s;
        int counter = 0;

        while ((s =  ReplayService.getNextTweet()) != null) {
            String[] words = s.toLowerCase().split(" ");
            for (int i = 0; i < words.length; i++) {
                uniques.add(words[i]);
            }
            counter++;
            if (counter % 100000 == 0) {
                System.out.println("tweets seen =" + counter+ ", unique words seen = " + uniques.size());
            }
        }

    }
}
