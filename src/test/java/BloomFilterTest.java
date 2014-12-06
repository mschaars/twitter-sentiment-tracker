import data.BloomFilterService;
import orestes.bloomfilter.BloomFilter;
import org.junit.Test;

/**
 * This class does not constitute full unit tests,
 * but is used to debug the way we use bloom filters.
 * The Bloom filter library comes with its
 * own full set of tests.
 */
public class BloomFilterTest {

    @Test
    public void testIncrement() {
        BloomFilter<String> stopWords = BloomFilterService.initFromFile("stopwords.txt", 0.02);

        System.out.println(stopWords.contains(","));

    }
}
