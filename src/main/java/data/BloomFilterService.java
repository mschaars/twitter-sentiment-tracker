package data;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;

/**
 * Creates and generates a bloom filter
 * from an input file.
 */
public class BloomFilterService {

    /**
     * Returns a Bloomfilter of the appropriate size for
     * some given error, containg all the words provided
     * in a file with one word per line.
     * @param fileName
     * @param eps
     * @return
     */
    public static BloomFilter<String> initFromFile(String fileName, double eps) {
        LinkedList<String> list = new LinkedList<>();

        try {

            File file = new File(fileName);
            FileReader reader = new FileReader(file);
            BufferedReader in = new BufferedReader(reader);
            String string;

            while ((string = in.readLine()) != null) {
                list.add(string.trim());
            }
        } catch (IOException e) {
        }

        BloomFilter<String> bf = new FilterBuilder(list.size(), eps).<String>buildBloomFilter();
        bf.addAll(list);

        return bf;
    }
}
