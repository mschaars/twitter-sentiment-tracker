package data;

/**
 * Wrapper for counter.
 */
public class FilterItem  {
    // Need fast access..
    public int totalCounter;
    public int matchCounter;

    public FilterItem(int totalCounter, int matchCounter) {
        this.totalCounter = totalCounter;
        this.matchCounter = matchCounter;
    }
}
