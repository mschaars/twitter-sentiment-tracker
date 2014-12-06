package data;

import com.google.common.util.concurrent.RateLimiter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Reads historical tweets from a file
 */
public class ReplayService {

    private static LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();
    private static RateLimiter rateLimiter;

    public static void init(int rate) {
        rateLimiter = RateLimiter.create(rate);
    }

    public static void readTweets(String fileName) {
        try {
            File file = new File(fileName);
            FileReader reader = new FileReader(file);
            BufferedReader in = new BufferedReader(reader);
            String string;

            while ((string = in.readLine()) != null) {
                String[] content = string.split(",");
                queue.offer(content[5]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Rate-limited polling of queue.
     * @return
     */
    public static String getNextTweet() {
        if (rateLimiter != null){
            rateLimiter.acquire();
        }
        return queue.poll();
    }
}
