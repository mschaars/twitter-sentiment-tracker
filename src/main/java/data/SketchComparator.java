package data;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Implementation found in:
 * Real-Time Analytics: Techniques to Analyze and Visualize Streaming Data
 * Byron Ellis
 */
public class SketchComparator<E extends Serializable> implements Comparator<E> {

    public CountMinSketch<E> sketch;

    public SketchComparator(CountMinSketch<E> sketch) {
        this.sketch = sketch;
    }

    @Override
    public int compare(E o1, E o2) {
        return (int) (sketch.getCount(o1) - sketch.getCount(o1));
    }
}
