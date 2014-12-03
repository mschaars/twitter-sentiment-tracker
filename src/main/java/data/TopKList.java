package data;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Created by Michael on 03.12.2014.
 */
public class TopKList<E extends Serializable> {
    CountMinSketch<E> sketch;
    PriorityQueue<E> heap;

    int k = Integer.MAX_VALUE;

    public TopKList(int k,CountMinSketch<E> sketch) {
        this.sketch = sketch;
        this.k = k;
        this.heap = new PriorityQueue<E>(k+1,new
                SketchComparator<E>(sketch));
    }

    public void add(E element) {
        sketch.increment(element);
        heap.add(element);
        while(heap.size() > k) {
            heap.remove();
        }
    }

    public List<E> getTopKElements() {
        if (heap != null) {
            List<E> list = new LinkedList<>();
            for (E e : heap) {
                list.add(e);
            }

            return list;
        }
        return null;
    }
}
