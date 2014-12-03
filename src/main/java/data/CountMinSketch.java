package data;

import java.io.Serializable;
import java.util.Random;

/**
 * Minimalistic implementation of the
 * count-min-sketch data structure.
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
     * @return
     */
    public void increment(Object obj) {
        for (int i = 0; i < depth; i++) {
            grid[i][hash(obj, i)]++;
        }
        size++;
    }

    public long getCount(Object obj) {
        long count = Integer.MAX_VALUE;
        for (int i = 0; i < depth; i++) {
            count = Math.min(count, grid[i][hash(obj, i)]);
        }
        return count;

    }

    private int hash(Object obj, int i) {
        long hash = hashes[i];
        return Math.abs(((int)(obj.hashCode() * hash)) % width);
    }

    public int getSize() {
        return size;
    }
}
