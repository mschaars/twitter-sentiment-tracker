package data;

import java.io.Serializable;
import java.util.Random;

/**
 * Custom implementation of the count-min-sketch data structure
 * for Strings.
 */
public class CountMinSketch<E extends Serializable> {
    private int width = 0;
    private int depth = 0;
    private int size = 0;

    private long[][] grid;
    private long[] hashes;

    public CountMinSketch(int width, int depth, int seed) {
        this.width = width;
        this.depth = depth;
        this.hashes = new long[depth];
        Random random = new Random(seed);

        for (int i = 0; i < depth; i++) {
            hashes[i] = random.nextInt(Integer.MAX_VALUE);
        }

        grid = new long[depth][width];
    }


    /**
     * Wordcounts are only incremented one at a time.
     * @param str
     * @return
     */
    public void increment(String str) {
        for (int i = 0; i < depth; i++) {
            grid[i][hash(str, i)]++;
        }
        size++;
    }

    public long getCount(String str) {
        long count = Integer.MAX_VALUE;
        for (int i = 0; i < depth; i++) {
            count = Math.min(count, grid[i][hash(str, i)]);
        }
        return count;

    }
    /**
     * String hashing.
     * @param str
     * @param i
     * @return
     */
    private int hash(String str, int i) {
        long hash = hashes[i];
        return Math.abs(((int)(str.hashCode() * hash)) % width);
    }
}
